/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.common.settings.Settings;

/**
 * This test compares behavior of a standard mode data stream and a logsdb data stream using stored source.
 * There should be no differences between such two data streams.
 */
public class BulkStoredSourceChallengeRestIT extends BulkChallengeRestIT {
    @Override
    public void contenderSettings(Settings.Builder builder) {
        super.contenderSettings(builder);
        builder.put("index.mapping.source.mode", "stored");
    }
}
