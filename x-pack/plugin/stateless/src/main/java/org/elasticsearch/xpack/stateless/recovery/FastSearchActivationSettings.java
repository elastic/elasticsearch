/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.Locale;

/**
 * Settings of the (prototype) fast cold activation path of a stateless search shard.
 * <p>
 * When {@link #FAST_ACTIVATION_ENABLED_SETTING} is enabled, a search shard recovering from the object store:
 * <ol>
 *     <li>does not discover its commit in the object store (no LIST of the shard's term containers and no read of the latest batched
 *     compound commit header) but registers with the indexing shard first and recovers from the compound commit returned in the
 *     registration response. The referenced compound commit headers are not read either: the commit's files are installed with their
 *     plain blob locations (as with {@code stateless.search.use_internal_files_replicated_content: false}). The existing object store
 *     discovery path is used as a fallback when the indexing shard returns no commit or the registration fails;</li>
 *     <li>does not run the recovery warming (header/footer warming per region and offline warming of whole blobs); instead it prefetches,
 *     in parallel and before opening the reader, only what opening the reader needs: whole blobs up to
 *     {@link #FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING} bytes, and the headers, footers and metadata files of the other blobs;</li>
 *     <li>fills the cache on demand (cache misses and the prefetch above) with ranges of {@link #FAST_ACTIVATION_RANGE_SIZE_SETTING}
 *     bytes instead of whole cache regions, except for vector files: their cache misses fill ranges of at least
 *     {@link #FAST_ACTIVATION_VECTOR_RANGE_SIZE_SETTING} bytes, and the first cache miss on a vector file (or a slice of it) of at most
 *     {@link #FAST_ACTIVATION_VECTOR_WHOLE_FILE_MAX_SETTING} bytes prefetches all of it (see
 *     {@link org.elasticsearch.xpack.stateless.cache.reader.VectorFillPolicy}).</li>
 * </ol>
 * The settings are read when the search shard starts recovering. Disabled by default.
 */
public final class FastSearchActivationSettings {

    private FastSearchActivationSettings() {}

    /**
     * Returns a fast-activation or parking setting, or its default when the node's cluster settings do not register it. Some test
     * harnesses build nodes without the plugin's settings; reading an unregistered setting throws, which would break recovery there
     * even though every one of these features is off by default.
     */
    public static <T> T getOrDefault(ClusterSettings clusterSettings, Setting<T> setting) {
        return clusterSettings.isDynamicSetting(setting.getKey()) ? clusterSettings.get(setting) : setting.getDefault(Settings.EMPTY);
    }

    public static final Setting<Boolean> FAST_ACTIVATION_ENABLED_SETTING = Setting.boolSetting(
        "stateless.fast_activation.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> FAST_ACTIVATION_RANGE_SIZE_SETTING = new Setting<>(
        "stateless.fast_activation.range_size",
        // 1 MiB: the measured best first fill (steps 2-3); 128 KiB multiplied the dependent query fills
        ByteSizeValue.ofMb(1).getStringRep(),
        s -> ByteSizeValue.parseBytesSizeValue(s, "stateless.fast_activation.range_size"),
        value -> {
            if (value.getBytes() <= 0L || value.getBytes() % SharedBytes.PAGE_SIZE != 0L) {
                throw new SettingsException(
                    "setting [{}] must be greater than zero and must be multiple of {}",
                    "stateless.fast_activation.range_size",
                    SharedBytes.PAGE_SIZE
                );
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING = Setting.byteSizeSetting(
        "stateless.fast_activation.whole_blob_threshold",
        ByteSizeValue.ofMb(4),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Minimum size of the cache fills of vector files on the fast activation path (capped at the cache range size), {@code 0} to fill
     * them like the other files with {@link #FAST_ACTIVATION_RANGE_SIZE_SETTING}.
     */
    public static final Setting<ByteSizeValue> FAST_ACTIVATION_VECTOR_RANGE_SIZE_SETTING = Setting.byteSizeSetting(
        "stateless.fast_activation.vector_range_size",
        ByteSizeValue.ofMb(16),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Largest vector file (or slice of one) that the fast activation path prefetches whole on its first cache miss, {@code 0} to disable.
     */
    public static final Setting<ByteSizeValue> FAST_ACTIVATION_VECTOR_WHOLE_FILE_MAX_SETTING = Setting.byteSizeSetting(
        "stateless.fast_activation.vector_whole_file_max",
        ByteSizeValue.ofMb(64),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How a fast-activated search shard finds the commit to open.
     */
    public enum Discovery {
        /**
         * Register with the indexing shard (one transport round trip) and recover from the commit it returns (step 2). Needs a resident
         * indexing shard with an engine; falls back to object store discovery (LIST + header reads) otherwise.
         */
        REGISTRATION,
        /**
         * Read the {@link org.elasticsearch.xpack.stateless.commits.CommitPointer} of the shard's current primary term with one GET and
         * open its commit, without asking the indexing shard; registration then runs in the background (step 2b). Falls back to object
         * store discovery if the pointer is missing, corrupt or does not match the cluster state.
         */
        POINTER
    }

    public static final Setting<Discovery> FAST_ACTIVATION_DISCOVERY_SETTING = new Setting<>(
        "stateless.fast_activation.discovery",
        Discovery.REGISTRATION.name().toLowerCase(Locale.ROOT),
        s -> Discovery.valueOf(s.toUpperCase(Locale.ROOT)),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether indexing shards (over)write a {@link org.elasticsearch.xpack.stateless.commits.CommitPointer} after each uploaded batched
     * compound commit. Read on the index nodes, at each upload.
     */
    public static final Setting<Boolean> COMMIT_POINTER_ENABLED_SETTING = Setting.boolSetting(
        "stateless.commit_pointer.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Phase 4 (prototype): the largest amount of open-time bytes
     * (see {@link org.elasticsearch.xpack.stateless.commits.CommitPointerOpenBytes})
     * that an indexing shard puts in its {@link org.elasticsearch.xpack.stateless.commits.CommitPointer}, {@code 0} (default) for none.
     * Above the bound the pointer carries no open-time bytes and activation warms headers from the object store as before. Read on the
     * index nodes, at each pointer write; the bytes are read from the object store on the index node (only those of new blobs).
     */
    public static final Setting<ByteSizeValue> COMMIT_POINTER_OPEN_BYTES_MAX_SIZE_SETTING = Setting.byteSizeSetting(
        "stateless.commit_pointer.open_bytes_max_size",
        ByteSizeValue.ZERO,
        ByteSizeValue.ZERO,
        ByteSizeValue.ofMb(32),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Phase 4 (prototype): whether a search shard activated from a commit pointer that carries open-time bytes writes them in the shared
     * cache and skips the header/footer/metadata warming. Only has an effect if the index nodes write such pointers
     * ({@link #COMMIT_POINTER_OPEN_BYTES_MAX_SIZE_SETTING}, off by default); exists to compare both on the same pointer.
     */
    public static final Setting<Boolean> FAST_ACTIVATION_USE_POINTER_OPEN_BYTES_SETTING = Setting.boolSetting(
        "stateless.fast_activation.use_pointer_open_bytes",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Fill size of the activation-time header/footer/metadata warming (independent of the query fill
     * {@link #FAST_ACTIVATION_RANGE_SIZE_SETTING}),
     * so that reading 1 KiB headers does not pull a whole query fill. Whole small blobs are still fetched with one GET each.
     */
    public static final Setting<ByteSizeValue> FAST_ACTIVATION_HEADER_RANGE_SIZE_SETTING = new Setting<>(
        "stateless.fast_activation.header_range_size",
        // measured (step 2b, d100k): 64 KiB doubles the GETs of the open prefetch (headers of a < 16 MiB BCC are not replicated next
        // to its header, so each one is its own small fill) and was 30-40% slower than 1 MiB on a thread-bound prefetch pool
        ByteSizeValue.ofMb(1).getStringRep(),
        s -> ByteSizeValue.parseBytesSizeValue(s, "stateless.fast_activation.header_range_size"),
        value -> {
            if (value.getBytes() <= 0L || value.getBytes() % SharedBytes.PAGE_SIZE != 0L) {
                throw new SettingsException(
                    "setting [{}] must be greater than zero and must be multiple of {}",
                    "stateless.fast_activation.header_range_size",
                    SharedBytes.PAGE_SIZE
                );
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How long after the start of a fast activation the small fill sizes (query fill, header fill, vector policy) apply to the shard.
     * Afterwards cache misses of the shard fill whole cache ranges again, also on inputs opened during the activation. {@code 0} keeps
     * the small fills for the lifetime of the shard (steps 2-3 behaviour).
     */
    public static final Setting<TimeValue> FAST_ACTIVATION_FILL_WINDOW_SETTING = Setting.timeSetting(
        "stateless.fast_activation.fill_window",
        TimeValue.timeValueSeconds(30),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Phase 5, item 1 (see {@link ActivationSortPrefetcher}): before a field-sorted query runs on a shard that is in its activation fill
     * window ({@link #FAST_ACTIVATION_FILL_WINDOW_SETTING}), create the sort fields' doc values on every segment so that their first
     * pages are prefetched in one wave instead of one segment after the other. Off by default.
     */
    public static final Setting<Boolean> FAST_ACTIVATION_PREFETCH_SORT_DOC_VALUES_SETTING = Setting.boolSetting(
        "stateless.fast_activation.prefetch_sort_doc_values",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Phase 5, item 3 (see {@link ActivationQueryRangesPrefetcher}): when a shard is activated from a commit pointer that carries its
     * open-time bytes, prefetch (asynchronously, not awaited) the small files that first queries read with dependent reads (terms index
     * and dictionary, stored fields index, points index, small vector graphs), up to this many bytes in total. {@code 0} (default) is off.
     */
    public static final Setting<ByteSizeValue> FAST_ACTIVATION_PREFETCH_QUERY_RANGES_MAX_SIZE_SETTING = Setting.byteSizeSetting(
        "stateless.fast_activation.prefetch_query_ranges_max_size",
        ByteSizeValue.ZERO,
        ByteSizeValue.ZERO,
        ByteSizeValue.ofMb(256),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Phase 5, item 4: when the open-time bytes from the commit pointer were seeded in the cache, do not warm whole small blobs
     * ({@link #FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING}) before opening the reader: the reader needs nothing else, so the shard
     * starts after the pointer GET only. Off by default (the whole small blobs are warmed, phase 4 behaviour).
     */
    public static final Setting<Boolean> FAST_ACTIVATION_SKIP_WHOLE_BLOB_WARMING_WITH_OPEN_BYTES_SETTING = Setting.boolSetting(
        "stateless.fast_activation.skip_whole_blob_warming_with_open_bytes",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
}
