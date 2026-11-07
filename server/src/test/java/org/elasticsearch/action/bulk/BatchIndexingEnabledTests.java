/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.containsString;

/** Tests the cluster-level {@code indices.batch_indexing} kill switch. */
public class BatchIndexingEnabledTests extends ESTestCase {

    public void testDefaultsToFeatureFlagState() {
        assertEquals(BatchIndexingEnabled.FEATURE_FLAG.isEnabled(), BatchIndexingEnabled.BATCH_INDEXING.get(Settings.EMPTY));
        BatchIndexingEnabled gate = new BatchIndexingEnabled(
            new ClusterSettings(Settings.EMPTY, Set.of(BatchIndexingEnabled.BATCH_INDEXING))
        );
        assertEquals(BatchIndexingEnabled.FEATURE_FLAG.isEnabled(), gate.isEnabled());
    }

    public void testKillSwitchDisablesBatchIndexing() {
        Settings off = Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), false).build();
        assertFalse(new BatchIndexingEnabled(new ClusterSettings(off, Set.of(BatchIndexingEnabled.BATCH_INDEXING))).isEnabled());
    }

    public void testEnablingRejectedWithoutFeatureFlag() {
        assumeFalse("only applies when the feature flag is off", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        Settings on = Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> BatchIndexingEnabled.BATCH_INDEXING.get(on));
        assertThat(e.getMessage(), containsString("batch_indexing feature flag"));
    }
}
