/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * This test compares behavior of a logsdb data stream and a standard index mode data stream
 * containing data reindexed from initial data stream.
 * There should be no differences between such two data streams.
 */
public class LogsDbVersusLogsDbReindexedIntoStandardModeChallengeRestIT extends ReindexChallengeRestIT {
    public String getBaselineDataStreamName() {
        return "logs-apache-baseline";
    }

    public String getContenderDataStreamName() {
        return "standard-apache-reindexed-contender";
    }

    @Override
    public void baselineSettings(Settings.Builder builder) {
        dataGenerationHelper.logsDbSettings(builder);
    }

    @Override
    public void contenderSettings(Settings.Builder builder) {

    }

    @Override
    public void baselineMappings(XContentBuilder builder) throws IOException {
        dataGenerationHelper.writeLogsDbMapping(builder);
    }

    @Override
    public void contenderMappings(XContentBuilder builder) throws IOException {
        dataGenerationHelper.writeStandardMapping(builder);
    }
}
