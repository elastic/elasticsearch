/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.logsEnabledAfterUpgrade;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.logsdb.templates.PatternTextTemplates;

public class PatternTextLogsEnabledAfterUpgradeRollingUpgradeIT extends AbstractStringTypeLogsEnabledAfterUpgradeTestCase {

    private static final String MIN_VERSION = "gte_v9.2.0";

    public PatternTextLogsEnabledAfterUpgradeRollingUpgradeIT(String template, String testScenario) {
        super(PatternTextTemplates.DATA_STREAM_NAME_PREFIX + "." + testScenario, template);
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return PatternTextTemplates.templates();
    }

    @Override
    protected void checkRequiredFeatures() {
        assumeTrue("pattern_text only available from 9.2.0 onward", oldClusterHasFeature(MIN_VERSION));
    }
}
