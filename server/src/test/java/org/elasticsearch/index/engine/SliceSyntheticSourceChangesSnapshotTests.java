/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceFieldMapper;

/**
 * Runs the slice-enabled changes-snapshot scenarios from {@link SliceChangesSnapshotTests} through
 * {@link LuceneSyntheticSourceChangesSnapshot} (synthetic recovery source). This exercises the synthetic snapshot's raw
 * compound-{@code _id} read on a slice delete tombstone and its plain-id+routing recovery of a slice index op.
 */
public class SliceSyntheticSourceChangesSnapshotTests extends SliceChangesSnapshotTests {

    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.name())
            .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
            .build();
    }
}
