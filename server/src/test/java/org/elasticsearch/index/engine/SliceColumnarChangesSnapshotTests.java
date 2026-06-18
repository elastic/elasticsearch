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
import org.elasticsearch.index.mapper.FieldMapper;

/**
 * Runs the slice-enabled changes-snapshot scenarios from {@link SliceChangesSnapshotTests} on an index that also uses
 * columnar {@code _id}
 */
public class SliceColumnarChangesSnapshotTests extends SliceChangesSnapshotTests {

    @Override
    public void setUp() throws Exception {
        assumeTrue(
            "columnar _id requires the extended doc values feature flag",
            FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled()
        );
        super.setUp();
    }

    @Override
    protected Settings indexSettings() {
        return Settings.builder().put(super.indexSettings()).put(IndexSettings.USE_COLUMNAR_ID_BY_DEFAULT.getKey(), true).build();
    }

    public void testIdMapperIsColumnar() {
        // Guard: the scenarios inherited above only exercise the slice+columnar paths if the index is actually columnar.
        assertTrue(
            "a slice-enabled index with the columnar default should use columnar _id",
            engine.engineConfig.getMapperService().mappingLookup().isColumnarId()
        );
    }
}
