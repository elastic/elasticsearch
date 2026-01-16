/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.logsEnabledFromStart;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.index.mapper.MapperFeatures;
import org.elasticsearch.xpack.logsdb.templates.MatchOnlyTextTemplates;

public class MatchOnlyTextLogsEnabledFromStartRollingUpgradeIT extends AbstractStringTypeLogsEnabledFromStartTestCase {

    public MatchOnlyTextLogsEnabledFromStartRollingUpgradeIT(String template, String testScenario) {
        super(MatchOnlyTextTemplates.DATA_STREAM_NAME_PREFIX + "." + testScenario, template);
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return MatchOnlyTextTemplates.templates();
    }

    @Override
    protected void checkRequiredFeatures() {
        assumeTrue(
            "Match only text block loader bug is present and fix is not present in this cluster",
            oldClusterHasFeature(MapperFeatures.MATCH_ONLY_TEXT_BLOCK_LOADER_FIX)
        );
    }
}
