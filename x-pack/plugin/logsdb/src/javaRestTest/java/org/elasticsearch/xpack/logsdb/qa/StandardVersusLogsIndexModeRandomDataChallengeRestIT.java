/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

/**
 * Challenge test (see {@link StandardVersusLogsIndexModeChallengeRestIT}) that uses randomly generated
 * mapping and documents in order to cover more code paths and permutations.
 */
public class StandardVersusLogsIndexModeRandomDataChallengeRestIT extends StandardVersusLogsIndexModeChallengeRestIT {
    protected final DataGenerationHelper dataGenerationHelper;

    public StandardVersusLogsIndexModeRandomDataChallengeRestIT() {
        this(new DataGenerationHelper());
    }

    protected StandardVersusLogsIndexModeRandomDataChallengeRestIT(DataGenerationHelper dataGenerationHelper) {
        super();
        this.dataGenerationHelper = dataGenerationHelper;
    }

    @Override
    public void baselineMappings(XContentBuilder builder) throws IOException {
        dataGenerationHelper.standardMapping(builder);
    }

    @Override
    public void contenderMappings(XContentBuilder builder) throws IOException {
        dataGenerationHelper.logsDbMapping(builder);
    }

    @Override
    public void contenderSettings(Settings.Builder builder) {
        super.contenderSettings(builder);
        dataGenerationHelper.logsDbSettings(builder);
    }

    @Override
    protected XContentBuilder generateDocument(final Instant timestamp) throws IOException {
        var document = XContentFactory.jsonBuilder();
        dataGenerationHelper.generateDocument(
            document,
            Map.of("@timestamp", DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(timestamp))
        );
        return document;
    }
}
