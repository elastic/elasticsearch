/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;

import static org.hamcrest.Matchers.containsString;

/**
 * Verifies that {@link EsqlPlugin#GROK_WATCHDOG_MAX_EXECUTION_TIME} is honored at execution time.
 * The setting is {@code NodeScope} only: every node reads its own local value when it builds the
 * GROK matcher used against real data (see {@code LocalExecutionPlanner#planGrok}), so there is no
 * need to carry the value in the ES|QL wire format.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class GrokWatchdogIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(EsqlPlugin.GROK_WATCHDOG_MAX_EXECUTION_TIME.getKey(), TimeValue.timeValueMillis(1))
            .build();
    }

    public void testCatastrophicBacktrackingPatternIsInterrupted() {
        prepareIndex("test").setId("1").setSource("message", "a".repeat(30) + "X").get();
        refresh("test");

        Exception e = expectThrows(Exception.class, () -> run("FROM test | GROK message \"(?<a>a+)+b\" | KEEP a"));
        assertThat(ExceptionsHelper.stackTrace(e), containsString("interrupted"));
    }
}
