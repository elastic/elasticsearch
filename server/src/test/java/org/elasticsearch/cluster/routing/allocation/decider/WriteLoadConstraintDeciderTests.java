/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.common.settings.Settings;

public class WriteLoadConstraintDeciderTests {

    public void testWriteLoadDeciderIsDisabled() {
        // TODO
    }

    public void testShardWithNoWriteLoadEstimateIsAlwaysYES() {
        Settings writeLoadConstraintSettings = Settings.builder()
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
            )
            .build();
        // TODO
    }

    public void testShardWithWriteLoadEstimate() {
        // TODO: test successful re-assignment and rejected re-assignment due to threshold
    }

}
