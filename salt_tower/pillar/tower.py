# pylint: disable=missing-docstring
"""
salt-tower ext_pillar module
"""

import copy
import errno
import fnmatch
import json
import logging
import os
import re
import shutil
import string
import zstandard as zstd
from glob import glob
from typing import NamedTuple

import salt.loader
import salt.minion
import salt.template
import salt.utils.context
from salt.utils.data import traverse_dict_and_list
from yaml.constructor import ConstructorError
from yaml.nodes import ScalarNode

from filelock import FileLock

LOGGER = logging.getLogger(__name__)


# try:
#     import compression.zstd as zstd
#     HAS_ZSTD = True
# except:
#     HAS_ZSTD = False
#     import compression.gzip as gzip


def _match_minion_impl(tgt, opts):
    matchers = salt.loader.matchers(dict(__opts__, **opts))  # pylint: disable=no-member
    return matchers["compound_match.match"](tgt)


def ext_pillar(minion_id, pillar, *args, **_kwargs):
    # `opts["pillarenv"]` usually is set, but to `None`. Therefore, we
    # cannot use `__opts__.get("pillarenv", "base")`.
    env = __opts__.get("pillarenv", None)

    if env is None:
        env = "base"

    tower = Tower(minion_id, env, pillar)

    for top in list(args):
        if not os.path.exists(top):
            LOGGER.warning("Tower top file `%s' does not exists", top)
            continue

        tower.run(top)

    return dict(tower)


class Optional(NamedTuple):
    pattern: str


def _yamlet_construct_optional(_loader, node):
    if isinstance(node, ScalarNode):
        return Optional(node.value)

    raise ConstructorError(  # pylint: disable=duplicate-code
        None,
        None,
        f"expected a scalar node, but found {node.id}",
        node.start_mark,
    )


class Tower(dict):
    def __init__(self, minion_id, env, pillar):
        super().__init__(pillar)

        self.env = env
        self.minion_id = minion_id

        opts = copy.copy(__opts__)
        opts["pillar"] = self

        self._renderers = salt.loader.render(opts, __salt__)
        self._formatter = Formatter(self)
        self._included = []

        if "yamlet" in self._renderers:
            self._default_renderers = "jinja|yamlet"
        else:
            LOGGER.warning(
                "Yamlet renderer not available. Tower functionality will be limited."
            )
            self._default_renderers = "jinja|yaml"

        if __opts__.get("salt_tower.cache.enable", False):
            lock_timeout = int(__opts__.get("salt_tower.cache.lock_timeout_seconds", 180))
            self._global_cache = RenderCache(self, lock_timeout_seconds=lock_timeout)
            self._minion_cache = RenderCache(self, minion_id=minion_id, lock_timeout_seconds=lock_timeout)
            self._global_cache_regex_matchers = []
            self._minion_cache_regex_matchers = []
            self._minion_match_all_nonglobal = False

            if __opts__.get("salt_tower.cache.global_paths_glob", False):
                glob_list = __opts__.get("salt_tower.cache.global_paths_glob")
                if isinstance(glob_list, str):
                    glob_list = [glob_list]

                for glob_str in glob_list:
                    # self._global_cache_regex_matchers.append(re.compile(glob.translate(glob_str, recursive=True, include_hidden=False)))
                    glob_str = glob_str.replace('**', '*')
                    self._global_cache_regex_matchers.append(re.compile(fnmatch.translate(glob_str)))

            if __opts__.get("salt_tower.cache.global_paths_regex", False):
                regex_list = __opts__.get("salt_tower.cache.global_paths_regex")
                if isinstance(regex_list, str):
                    regex_list = [regex_list]

                for regex_str in regex_list:
                    self._global_cache_regex_matchers.append(re.compile(regex_str))

            if __opts__.get("salt_tower.cache.minion_paths_glob", False):
                glob_list = __opts__.get("salt_tower.cache.minion_paths_glob")
                if isinstance(glob_list, str):
                    glob_list = [glob_list]

                for glob_str in glob_list:
                    # self._minion_cache_regex_matchers.append(re.compile(glob.translate(glob_str, recursive=True, include_hidden=False)))
                    glob_str = glob_str.replace('**', '*')
                    self._minion_cache_regex_matchers.append(re.compile(fnmatch.translate(glob_str)))

            if __opts__.get("salt_tower.cache.minion_paths_regex", False):
                regex_list = __opts__.get("salt_tower.cache.minion_paths_regex")
                if isinstance(regex_list, str):
                    regex_list = [regex_list]

                for regex_str in regex_list:
                    self._minion_cache_regex_matchers.append(re.compile(regex_str))

            if len(self._minion_cache_regex_matchers) == 0:
                self._minion_match_all_nonglobal = True

    def get(self, key, default=None, require=False, **kwargs):
        if require:
            default = KeyError(f"Pillar key missing: {key}")
        value = traverse_dict_and_list(self, key, default, **kwargs)
        if isinstance(value, KeyError):
            raise value  # pylint: disable=raising-non-exception
        return value

    def update(self, obj, merge=True, **kwargs):
        if merge:
            return self.merge(self, obj, **kwargs)
        return super().update(obj)

    def merge(self, *args, **kwargs):
        return _merge(*args, **kwargs)

    def format(self, obj, *args, **kwargs):
        if isinstance(obj, dict):
            return {k: self.format(v, *args, **kwargs) for k, v in obj.items()}

        if isinstance(obj, list):
            return [self.format(i, *args, **kwargs) for i in obj]

        if isinstance(obj, bytes):
            return obj

        if isinstance(obj, str):
            try:
                return self._formatter.format(obj, *args, **kwargs)
            except ValueError:
                return obj

        return obj

    def run(self, top):
        LOGGER.debug("Process tower top file `%s'", top)

        base = os.path.dirname(top)

        try:
            for item in self._load_top(top, base):
                if isinstance(item, dict):
                    for tgt, items in item.items():
                        if not isinstance(items, list):
                            raise ValueError(
                                f"tower.sls: include items must be a list but is {type(items).__name__}"
                            )

                        if not self._match_minion(tgt):
                            continue

                        for itm in items:
                            self._load_item(base, itm)
                else:
                    self._load_item(base, item)
        finally:
            if __opts__.get("salt_tower.cache.enable", False):
                if self._global_cache.is_open():
                    self._global_cache.close()
                if self._minion_cache.is_open():
                    self._minion_cache.close()

        if __opts__.get("salt_tower.enable_inventory", False) and "inventory" in self:
            del self["inventory"]


    def _match_minion(self, tgt):
        try:
            if hasattr(__grains__, "value"):
                grains = __grains__.value()
            else:
                grains = __grains__
            return _match_minion_impl(
                tgt, {"grains": grains, "pillar": dict(self), "id": self.minion_id}
            )
        except Exception:  # pylint: disable=broad-except
            LOGGER.exception("Unable to match target `%s'", tgt)
            return False

    def _load_top(self, top, base):
        data = self._compile(
            top,
            context={"basedir": base},
            _yamlet_constructors={"!optional": _yamlet_construct_optional},
        )

        if not isinstance(data, dict):
            LOGGER.critical("Tower top must be a dict, but is %s.", type(data))
            return []

        if self.env not in data:
            LOGGER.warning(
                "Tower top `%s' does not include env %s, skipping.", top, self.env
            )
            return []

        if not isinstance(data[self.env], list):
            LOGGER.critical(
                "Tower top `%s' env %s must be a list, but is %s.",
                top,
                self.env,
                type(data[self.env]),
            )
            return []

        return data[self.env]

    def _load_item(self, base, item):
        if isinstance(item, dict):
            self.update(item, merge=True)
        elif isinstance(item, str):
            self.load(item, base)
        elif isinstance(item, Optional):
            self.load(item.pattern, base, optional=True)
        else:
            raise ValueError(f"Invalid item type: {type(item).__name__}")

    def lookup(self, item, base=None, cwd=None):
        if cwd and item.startswith(("./", "../")):
            path = os.path.join(cwd, self.format(item))
        elif base:
            path = os.path.join(base, self.format(item))
        else:
            path = self.format(item)

        match = glob(path)

        if match:
            match = [i for i in match if os.path.isfile(i)]

        if match:
            LOGGER.debug("Found glob match: %s", match)
            return sorted(match)

        mode = __opts__.get("salt_tower.include_directory_mode")
        # This mode first checks if the given path is a directory. If
        # yes, all `*.sls` files inside the directory will be returned,
        # ordered by filename.
        if mode == "all-sls" and os.path.isdir(path):
            LOGGER.debug("Found directory match: %s", path)
            return sorted(
                [
                    file
                    for file in glob(os.path.join(path, "*.sls"))
                    if os.path.isfile(file)
                ]
            )

        for match in [path, f"{path}.sls", f"{path}/init.sls"]:
            if os.path.isfile(match):
                LOGGER.debug("Found file match: %s", match)
                return [match]

        return []

    def load(self, item, base=None, cwd=None, optional=False):
        matches = self.lookup(item, base, cwd)

        if (
            not matches
            and not optional
            and __opts__.get("salt_tower.raise_on_missing_files")
        ):
            raise FileNotFoundError(f"File not found: {item}")

        for file in matches:
            self._load_file(file, base)

    def _load_file(self, file, base=None):
        """
        Try to load fully qualified file path as a tower data file.

        It will first check if the file has already been loaded and, if not,
        if it exist in the filesystem.

        The file will be compiled and run as a salt template and is expected
        to return a dict.

        If the dict includes an `include` item or list, these includes will
        loaded before the loaded data is merged into the current pillar.
        """
        if file in self._included:
            LOGGER.warning("Skipping already included file: %s", file)
            return

        if not os.path.isfile(file):
            LOGGER.warning("Skipping non-existing file: %s", file)
            return

        LOGGER.debug("Tower: loading file: %s", file)

        self._included.append(file)

        data = None

        if __opts__.get("salt_tower.cache.enable", False):
            LOGGER.debug("Tower: caching is enabled (processing file: %s)", file)

            matched_global = False
            matched_minion = False

            for rgx in self._global_cache_regex_matchers:
                if rgx.match(file):
                    matched_global = True
                    break

            if not matched_global:
                if self._minion_match_all_nonglobal:
                    matched_minion = True
                else:
                    for rgx in self._minion_cache_regex_matchers:
                        if rgx.match(file):
                            matched_minion = True
                            break

            stat = os.stat(file)
            mtime = stat.st_mtime_ns

            if matched_global:
                LOGGER.debug("Tower: cache: processing file as global: %s", file)

                try:
                    data = self._global_cache.get(file, mtime)
                except RenderCacheClosedException:
                    LOGGER.warning(f"Trying to process path '{file}', but the global render cache is already closed.")

            elif matched_minion:
                LOGGER.debug("Tower: cache: processing file as minion-specific: %s", file)

                if self._global_cache.is_open():
                    LOGGER.debug("Tower: cache: global cache is still open.  Closing since we're now moving on to minion-specific files.")
                    self._global_cache.close()

                try:
                    data = self._minion_cache.get(file, mtime)
                except RenderCacheClosedException:
                    LOGGER.warning(f"Trying to process path '{file}', but the minion render cache is already closed.")
                # except RenderCacheFileAlreadyProcessedException:
                #     LOGGER.warning(f"Trying to process path '{file}', but this path has already been processed by the minion render cache.")
                #     self._minion_cache.close()

            else:
                LOGGER.debug("Tower: cache: not searching caches for file '%s' since it didn't match any patterns.", file)

                if self._global_cache.is_open():
                    LOGGER.debug("Tower: cache: global cache is still open.  Closing since we've now encountered something that doesn't fit any patterns.")
                    self._global_cache.close()

                if self._minion_cache.is_open():
                    LOGGER.debug("Tower: cache: minion cache is still open.  Closing since we've now encountered something that doesn't fit any patterns.")
                    self._minion_cache.close()

            if data is None:
                LOGGER.debug("Tower: cache: received no data from cache for file '%s'", file)
            else:
                LOGGER.debug("Tower: cache: received the following data from cache for file '%s':\n%s", file, json.dumps(data))

        if data is None:
            LOGGER.debug("Tower: processing sls/template: %s", file)
            data = self._compile(file, context={"basedir": base})

            if not isinstance(data, dict):
                LOGGER.debug("Loading %s did not return dict, but %s", file, type(data))
                return

            if "include" in data:
                includes = data.pop("include")

                if not isinstance(includes, list):
                    includes = [includes]

                for include in includes:
                    self.load(include, base, cwd=os.path.dirname(file))

            if __opts__.get("salt_tower.cache.enable", False):
                if matched_global and self._global_cache.is_valid():
                    LOGGER.debug("Tower: cache: adding data from file '%s' to global cache", file)
                    self._global_cache.put(file, mtime, data)
                elif matched_minion and self._minion_cache.is_valid():
                    LOGGER.debug("Tower: cache: adding data from file '%s' to minion-specific cache for minion '%s'", file, self.minion_id)
                    self._minion_cache.put(file, mtime, data)

        self.update(data, merge=True)

    def _compile(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        template,
        default=None,
        blacklist=None,
        whitelist=None,
        context=None,
        **kwargs,
    ):
        if default is None:
            default = self._default_renderers

        ctx = {}

        if context:
            ctx.update(context)

        ctx["tmplpath"] = template
        ctx["tmpldir"] = os.path.dirname(template)
        ctx["minion_id"] = self.minion_id
        ctx["pillar"] = self
        ctx["tower"] = self
        ctx["env"] = self.env

        if __opts__.get("salt_tower.enable_inventory", False):
            ctx["inventory"] = Inventory(self)

        def render(path, context=None, renderer="text"):
            if isinstance(context, dict):
                context = {**ctx, **context}
            else:
                context = ctx

            if "basedir" in context:
                path = os.path.join(context["basedir"], path)

            file = os.path.abspath(path)

            if not os.path.isfile(file):
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), file)

            return self._compile(file, renderer, None, None, context)

        ctx["render"] = render

        kwargs["tower"] = ctx["tower"]
        kwargs["minion_id"] = ctx["minion_id"]

        # Explicitly set `saltenv=None` to disable any special behavior
        # of some rendering engines (mostly JINJA) regarding rendering
        # states or pillars.
        #
        # Salt does not provide an option to pass any file lookup
        # customization to the renderers. Therefore some renderering
        # engines strictly assume that if `saltenv` is not `None` they
        # are rendering salt states, and if the internal variable
        # `_pillar_rend` is set, they are rendering pillars. They will
        # then use a different file lookup mechanism using the
        # configured pillar_roots or file_roots. This cannot be
        # customized.
        #
        # It is currently impossible to lookup files relative to the
        # tower.sls directory (which might or might not be also a
        # pillar_roots) for these renderers without explicitly using a
        # helper function.
        kwargs["saltenv"] = None
        kwargs["_pillar_rend"] = True

        if __opts__.get("salt_tower.unstable_enable_saltenv", False):
            kwargs["saltenv"] = self.env

        try:
            return salt.template.compile_template(
                template=template,
                renderers=self._renderers,
                default=default,
                blacklist=blacklist,
                whitelist=whitelist,
                context=ctx,
                **kwargs,
            )
        except Exception:
            LOGGER.critical("Unable to render template `%s'", template, exc_info=True)
            raise


class Inventory:  # pylint: disable=too-few-public-methods
    def __init__(self, tower):
        self._tower = tower

    def get(self, key, minion=None, require=True, **kwargs):
        if not minion:
            minion = self._tower.minion_id
        return self._tower.get(f"inventory:{minion}:{key}", require=require, **kwargs)


class Formatter(string.Formatter):
    def __init__(self, tower):
        self._tower = tower

    def get_field(self, field_name, args, kwargs):
        if field_name in kwargs:
            return (kwargs[field_name], None)

        value = self._tower.get(field_name, delimiter=".")

        if value is None:
            return ("{" + field_name + "}", None)

        return (value, None)


def _merge(tgt, *objects, strategy="merge-last"):
    for obj in objects:
        if isinstance(tgt, dict):
            tgt = _merge_dict(tgt, obj, strategy)
        elif isinstance(tgt, list):
            tgt = _merge_list(tgt, obj, strategy)
        else:
            raise TypeError(f"Cannot merge {type(tgt)}")

    return tgt


def _merge_clean(obj):
    if isinstance(obj, dict):
        return {k: _merge_clean(v) for k, v in obj.items() if k != "__"}

    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict) and len(obj[0]) == 1 and "__" in obj[0]:
            obj = obj[1:]
        return [_merge_clean(v) for v in obj]

    return obj


def _merge_dict(tgt, obj, strategy="merge-last"):  # pylint: disable=too-many-branches
    if not isinstance(obj, dict):
        raise TypeError(f"Cannot merge non-dict type, but is {type(obj)}")

    if "__" in obj:
        strategy = obj.pop("__")

    if strategy == "remove":
        for k in obj:
            if k in tgt:
                tgt.pop(k)

    elif strategy == "merge-last":
        for key, val in obj.items():
            if key in tgt and isinstance(tgt[key], dict) and isinstance(val, dict):
                _merge_dict(tgt[key], val, strategy)
            elif key in tgt and isinstance(tgt[key], list) and isinstance(val, list):
                _merge_list(tgt[key], val, strategy)
            else:
                tgt[key] = _merge_clean(val)

    elif strategy == "merge-first":
        for key, val in obj.items():
            if key in tgt and isinstance(tgt[key], dict) and isinstance(val, dict):
                _merge_dict(tgt[key], val, strategy)
            elif key in tgt and isinstance(tgt[key], list) and isinstance(val, list):
                _merge_list(tgt[key], val, strategy)
            elif key not in tgt:
                tgt[key] = _merge_clean(val)

    elif strategy == "overwrite":
        tgt.clear()
        tgt.update(_merge_clean(obj))

    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    return tgt


def _merge_list(tgt, lst, strategy="merge-last"):
    if not isinstance(lst, list):
        raise TypeError(f"Cannot merge non-list type, but is {type(lst)}")

    if lst and isinstance(lst[0], dict) and len(lst[0]) == 1 and "__" in lst[0]:
        strategy = lst.pop(0)["__"]

    if strategy == "remove":
        for val in lst:
            if val in tgt:
                tgt.remove(val)

    elif strategy == "merge-last":
        tgt.extend(_merge_clean(lst))

    elif strategy == "merge-first":
        for val in _merge_clean(lst):
            tgt.insert(0, val)

    elif strategy == "overwrite":
        del tgt[:]
        tgt.extend(_merge_clean(lst))

    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    return tgt


### What does cache do?
# - for each sls or other file processed by Tower._load_file()/Tower.load()/Tower._compile(), a cache key is created with the name of that file (or path to that file?).
#   - the value of that key is a dict containing:
#     - mtime: the mtime of the file when it was processed
#     - predecessor_info: a list containing a dict for each predecessor file (file whose data was used to compile the tower/pillar context that was used when generating this file's cached tower/pillar data) containing:
#       - slspath: path to the predecessor file
#       - mtime: the mtime of the predecessor file at the time it was processed
#     - data: the pillar/tower data as it existed when that sls/other file was last processed/compiled
# - when the empty cache is initially populated, the Tower class should call RenderCache.add(cache_entry) for each newly compiled file
#   - cache_entry = RenderCacheEntry(slspath, predecessor_info, data)  # where predecessor_info is a list of the cache entries processed before this during this run
# - when it's run any time after (i.e., when the cache is not empty), we need to determine if the cache is still valid for any file or each file
#
### How do we know when the cache is valid for a given file?  All of the following must be true:
# - no file exists that is alphanumerically prior to this one that:
#   - is new (does not exist in cached predecessor_info for this file)
#   - has changed/been reprocessed (different mtime in current predecessor_info than in cached predecessor_info for this file)
#   - has been deleted (present in cached predecessor_info for this file but missing from the filesystem/new predecessor_info)
#


### structure
# global:
#   cachedir/global/lock
#   cachedir/global/{cur,new}/<slspath>/mtime - empty file, but its mtime is set to mtime of source file of relevant entry into cache
#   cachedir/global/{cur,new}/<slspath>/info.json.zst
#   cachedir/global/{cur,new}/<slspath>/data.json.zst
# minion:
#   cachedir/minion/<minion_id>/lock
#   cachedir/minion/<minion_id>/{cur,new}/<slspath>/mtime - empty file, but its mtime is set to mtime of source file of relevant entry into cache
#   cachedir/minion/<minion_id>/{cur,new}/<slspath>/info.json.zst
#   cachedir/minion/<minion_id>/{cur,new}/<slspath>/data.json.zst

class RenderCacheClosedException(Exception):
    pass


class RenderCache:
    def __init__(self, tower, minion_id=None, lock_timeout_seconds=None):
        self._tower = tower
        self._minion_id = minion_id

        self._info = []

        self._cache_base_dir = os.path.join(__opts__["cachedir"], "tower")

        if minion_id:
            self._cache_dir = os.path.join(self._cache_base_dir, 'minion', minion_id)
            self._global = False
            self._global_cache_dir = os.path.join(self._cache_base_dir, 'global')
            self._global_cur_dir = os.path.join(self._global_cache_dir, 'cur')
            self._global_info_path = os.path.join(self._global_cur_dir, 'info.jsonz')
        else:
            self._cache_dir = os.path.join(self._cache_base_dir, 'global')
            self._global = True

        os.makedirs(self._cache_dir, exist_ok=True)

        self._lockfile = os.path.join(self._cache_dir, 'lock')

        self._cur_dir = os.path.join(self._cache_dir, 'cur')
        self._new_dir = os.path.join(self._cache_dir, 'new')

        lock_args = {
                'poll_interval': 0.25,
            }
        if lock_timeout_seconds:
            lock_args['lifetime'] = lock_timeout_seconds
        self._lock = FileLock(self._lockfile, **lock_args)

        self._cache_is_open = False
        self._cache_is_closed = False

    def _get_cur_info_path(self, slspath):
        # return os.path.join(self._cur_dir, slspath, 'info.jsonz')
        path = os.path.join(self._cur_dir, slspath.removeprefix('/'), 'info.jsonz')
        self._debug(f"_get_cur_info_path(slspath='{slspath}')", f"returning '{path}' (_cur_dir: '{self._cur_dir}')")
        return path

    def _get_new_info_path(self, slspath):
        # return os.path.join(self._new_dir, slspath, 'info.jsonz')
        path = os.path.join(self._new_dir, slspath.removeprefix('/'), 'info.jsonz')
        self._debug(f"_get_new_info_path(slspath='{slspath}')", f"returning '{path}' (_new_dir: '{self._new_dir}')")
        return path

    def _get_cur_data_path(self, slspath):
        # return os.path.join(self._cur_dir, slspath, 'data.jsonz')
        path = os.path.join(self._cur_dir, slspath.removeprefix('/'), 'data.jsonz')
        self._debug(f"_get_cur_data_path(slspath='{slspath}')", f"returning '{path}' (_cur_dir: '{self._cur_dir}')")
        return path

    def _get_new_data_path(self, slspath):
        # return os.path.join(self._new_dir, slspath, 'data.jsonz')
        path = os.path.join(self._new_dir, slspath.removeprefix('/'), 'data.jsonz')
        self._debug(f"_get_new_data_path(slspath='{slspath}')", f"returning '{path}' (_new_dir: '{self._new_dir}')")
        return path

    def get(self, slspath, mtime):
        self._debug(f"get(slspath='{slspath}', mtime={mtime})", "Entered function")
        self._check_cache_validity()

        cached_info_path = self._get_cur_info_path(slspath)
        cached_info_mtime = self._get_mtime(cached_info_path)
        if mtime != cached_info_mtime:
            self._debug(f"get(slspath='{slspath}', mtime={mtime})", "mtime mismatch ('{mtime}' (source), '{cached_info_mtime}' (cached)): returning None")
            return None

        cached_info = self._read_compressed_json(cached_info_path)
        if (cached_info is None) or (len(cached_info) != len(self._info)):
            self._debug(f"get(slspath='{slspath}', mtime={mtime})", "cached_info mismatch (1): returning None")
            return None

        for cached_item, live_item in zip(cached_info, self._info):
            if cached_item != live_item:
                self._debug(f"get(slspath='{slspath}', mtime={mtime})", "cached_info mismatch (2): returning None")
                return None

        cur_data_path = self._get_cur_data_path(slspath)
        data = self._read_compressed_json(cur_data_path)
        self._debug(f"get(slspath='{slspath}', mtime={mtime})", "Read cached data from disk")

        if data is None:
            self._debug(f"get(slspath='{slspath}', mtime={mtime})", "Cached data from disk was None")
            return None

        self._debug(f"get(slspath='{slspath}', mtime={mtime})", f"Copying '{os.path.join(self._cur_dir, slspath.removeprefix('/'))}' to '{os.path.join(self._new_dir, slspath.removeprefix('/'))}'")
        self._linkcopy_dir(os.path.join(self._cur_dir, slspath.removeprefix('/')), os.path.join(self._new_dir, slspath.removeprefix('/')))
        self._info.append(dict(slspath=slspath, mtime=mtime))

        self._debug(f"get(slspath='{slspath}', mtime={mtime})", "Returning data")
        return data

    def put(self, slspath, mtime, data):
        self._debug(f"put(slspath='{slspath}', mtime={mtime}, data=...)", 'Entered function')
        self._check_cache_validity()
        self._debug(f"put(slspath='{slspath}', mtime={mtime}, data=...)", f"Writing self._info to '{self._get_new_info_path(slspath)}'")
        self._write_compressed_json(self._get_new_info_path(slspath), self._info, mtime)
        # self._write_compressed_json(self._get_new_data_path(slspath), data, mtime)
        self._debug(f"put(slspath='{slspath}', mtime={mtime}, data=...)", f"Writing data to '{self._get_new_data_path(slspath)}'")
        self._write_compressed_json(self._get_new_data_path(slspath), data)
        self._info.append(dict(slspath=slspath, mtime=mtime))
        self._debug(f"put(slspath='{slspath}', mtime={mtime}, data=...)", 'Leaving function')

    def is_open(self):
        return self._cache_is_open

    def is_closed(self):
        return self._cache_is_closed

    def is_valid(self):
        return not self._cache_is_closed

    def open(self):
        if self._cache_is_open:
            self._debug(f"_open()", 'Already open')
            return
        elif self._cache_is_closed:
            self._debug(f"_open()", 'Already closed; raising RenderCacheClosedException')
            raise RenderCacheClosedException()

        self._debug(f"_open()", f"Acquiring lock: {self._lockfile}")
        self._lock.acquire()
        self._debug(f"_open()", f"Lock acquired: {self._lockfile}")

        self._create_clean_dir(self._new_dir)

        # if not self._global:
        #     self._debug(f"_open()", f"Not global, so deleting '{self._new_dir}' (if present) and copying '{self._global_cur_dir}' into its place to serve as the base, then loading .")
        #     self._rm(self._new_dir)
        #     self._linkcopy_dir(self._global_cur_dir, self._new_dir)
        # else:
        #     self._create_clean_dir(self._new_dir)

        if not self._global:
            self._debug(f"_open()", f"Not global, so loading info from global cache file at '{self._global_info_path}'.")
            self._info = self._read_compressed_json(self._global_info_path)

        self._cache_is_open = True
        self._debug(f"_open()", f"Opened")

    def close(self):
        self._debug(f"close()", 'Entered function')
        self._cache_is_closed = True
        if self._cache_is_open:
            self._debug(f"close()", 'Cache is open; closing...')

            self._debug(f"close()", f"Deleting '{self._cur_dir}'")
            self._rm(self._cur_dir)

            if self._global:
                info_path = os.path.join(self._new_dir, 'info.jsonz')
                self._debug(f"close()", f"Writing final file info to '{info_path}'")
                self._write_compressed_json(info_path, self._info)

            self._debug(f"close()", f"Copying '{self._new_dir}' to '{self._cur_dir}'")
            self._linkcopy_dir(self._new_dir, self._cur_dir)

            self._debug(f"close()", f"Deleting '{self._new_dir}'")
            self._rm(self._new_dir)

            self._cache_is_open = False

            self._debug(f"close()", f"Releasing lock: {self._lockfile}")
            self._lock.release()
            self._rm(self._lockfile)

    def _check_cache_validity(self):
        self._debug(f"_check_cache_validity()", 'Entered function')
        if not self.is_open():
            if self.is_valid():
                self.open()
            else:
                self._debug(f"_check_cache_validity()", 'Cache is invalid; raising RenderCacheClosedException')
                raise RenderCacheClosedException()
        self._debug(f"_check_cache_validity()", "Done (if you're seeing this, the cache is valid.)")

    def _rm(self, path):
        if os.path.exists(path):
            if os.path.isdir(path):
                self._debug(f"_rm(path='{path}')", 'Deleting directory')
                shutil.rmtree(path)
            else:
                self._debug(f"_rm(path='{path}')", 'Deleting file')
                os.unlink(path)

    def _create_clean_dir(self, path):
        self._debug(f"_open(path='{path}')", 'Entered function')
        self._rm(path)
        self._debug(f"_open(path='{path}')", 'Making directory')
        os.makedirs(path, exist_ok=True)

    def _linkcopy_dir(self, src, dst):
        shutil.copytree(src, dst, copy_function=os.link)

    def _get_mtime(self, path):
        if os.path.isfile(path):
            return os.stat(path).st_mtime_ns
        return None

    def _set_mtime(self, path, mtime):
        os.utime(path, ns=(mtime, mtime))

    def _read_compressed_json(self, path):
        # global HAS_ZSTD

        if not os.path.isfile(path):
            self._debug(f"_read_compressed_json(path='{path}')", 'Path does not exist')
            return None

        # if HAS_ZSTD:
        #     with zstd.open(path, mode='rt', encoding='utf-8') as f:
        #         return json.load(f)
        # else:
        #     with gzip.open(path, mode='rt', encoding='utf-8') as f:
        #         return json.load(f)

        with zstd.open(path, mode='r', encoding='utf-8') as f:
            self._debug(f"_read_compressed_json(path='{path}')", 'Reading file')
            return json.load(f)

    def _write_compressed_json(self, path, data, mtime=None):
        # global HAS_ZSTD

        if os.path.exists(path):
            self._debug(f"_write_compressed_json(path='{path}', mtime={mtime})", 'File already exists; deleting...')
            self._rm(path)
        else:
            dirpath = os.path.dirname(path)
            if not os.path.isdir(dirpath):
                if os.path.exists(dirpath):
                    self._debug(f"_write_compressed_json(path='{path}', mtime={mtime})", 'Parent directory already exists; deleting...')
                    self._rm(dirpath)
                self._debug(f"_write_compressed_json(path='{path}', mtime={mtime})", 'Creating parent directory')
                os.makedirs(dirpath, exist_ok=True)

        # if HAS_ZSTD:
        #     with zstd.open(path, mode='wt', encoding='utf-8', level=2) as f:
        #         json.dump(data, f)
        # else:
        #     with gzip.open(path, mode='wt', encoding='utf-8', compresslevel=3) as f:
        #         json.dump(data, f)

        with zstd.open(path, mode='w', encoding='utf-8', cctx=zstd.ZstdCompressor(level=2)) as f:
            self._debug(f"_write_compressed_json(path='{path}', mtime={mtime})", 'Writing file')
            json.dump(data, f)

        if mtime:
            self._debug(f"_write_compressed_json(path='{path}', mtime={mtime})", 'Setting mtime')
            self._set_mtime(path, mtime)

    def _debug(self, func, msg):
        if self._global:
            name = 'global'
        else:
            name = self._minion_id
        LOGGER.debug(f"Tower: RenderCache ({name}) -> {func}: {msg}")

