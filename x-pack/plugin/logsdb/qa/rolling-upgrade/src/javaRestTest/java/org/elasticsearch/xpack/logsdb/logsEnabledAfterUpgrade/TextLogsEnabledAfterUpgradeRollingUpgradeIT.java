/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.logsEnabledAfterUpgrade;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.logsdb.templates.TextTemplates;

public class TextLogsEnabledAfterUpgradeRollingUpgradeIT extends AbstractStringTypeLogsEnabledAfterUpgradeTestCase {

    public TextLogsEnabledAfterUpgradeRollingUpgradeIT(String template, String testScenario) {
        super(TextTemplates.DATA_STREAM_NAME_PREFIX + "." + testScenario, template);
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return TextTemplates.templates();
    }
}
