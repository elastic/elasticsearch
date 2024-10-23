/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.common.settings.Settings;

public class StandardVersusLogsIndexModeRandomDataDynamicMappingChallengeRestIT extends
    StandardVersusLogsIndexModeRandomDataChallengeRestIT {
    public StandardVersusLogsIndexModeRandomDataDynamicMappingChallengeRestIT() {
        super(new DataGenerationHelper(builder -> builder.withFullyDynamicMapping(true)));
    }

    @Override
    public void contenderSettings(Settings.Builder builder) {
        super.contenderSettings(builder);
        // ignore_dynamic_beyond_limit is set in the template so it's always true
        builder.put("index.mapping.total_fields.limit", randomIntBetween(1, 5000));
    }
}
