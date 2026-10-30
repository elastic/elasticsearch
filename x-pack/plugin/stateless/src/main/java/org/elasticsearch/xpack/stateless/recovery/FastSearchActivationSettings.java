/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * Settings of the (prototype) fast cold activation path of a stateless search shard ("cheap shards").
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
 *     bytes instead of whole cache regions.</li>
 * </ol>
 * The settings are read when the search shard starts recovering. Disabled by default.
 */
public final class FastSearchActivationSettings {

    private FastSearchActivationSettings() {}

    public static final Setting<Boolean> FAST_ACTIVATION_ENABLED_SETTING = Setting.boolSetting(
        "stateless.cheap_shard.fast_activation.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> FAST_ACTIVATION_RANGE_SIZE_SETTING = new Setting<>(
        "stateless.cheap_shard.fast_activation.range_size",
        ByteSizeValue.ofKb(128).getStringRep(),
        s -> ByteSizeValue.parseBytesSizeValue(s, "stateless.cheap_shard.fast_activation.range_size"),
        value -> {
            if (value.getBytes() <= 0L || value.getBytes() % SharedBytes.PAGE_SIZE != 0L) {
                throw new SettingsException(
                    "setting [{}] must be greater than zero and must be multiple of {}",
                    "stateless.cheap_shard.fast_activation.range_size",
                    SharedBytes.PAGE_SIZE
                );
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING = Setting.byteSizeSetting(
        "stateless.cheap_shard.fast_activation.whole_blob_threshold",
        ByteSizeValue.ofMb(4),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
}
