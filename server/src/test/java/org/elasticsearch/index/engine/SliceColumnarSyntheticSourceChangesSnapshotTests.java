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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.junit.Before;

/**
 * The slice + columnar {@code _id} scenarios from {@link SliceColumnarChangesSnapshotTests} run through
 * {@link LuceneSyntheticSourceChangesSnapshot} (synthetic recovery source). This exercises the synthetic snapshot's
 * columnar read of the compound identity term (both live docs and delete tombstones) from binary doc values.
 */
public class SliceColumnarSyntheticSourceChangesSnapshotTests extends SliceSyntheticSourceChangesSnapshotTests {

    @Before
    public void assumeColumnarEnabled() {
        assumeTrue("columnar _id requires the extended doc values feature flag", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Settings indexSettings() {
        return Settings.builder().put(super.indexSettings()).put(IndexSettings.USE_COLUMNAR_ID_BY_DEFAULT.getKey(), true).build();
    }

    public void testIdMapperIsColumnar() {
        assertTrue(
            "a slice-enabled index with the columnar default should use columnar _id",
            engine.engineConfig.getMapperService().mappingLookup().isColumnarId()
        );
    }
}
