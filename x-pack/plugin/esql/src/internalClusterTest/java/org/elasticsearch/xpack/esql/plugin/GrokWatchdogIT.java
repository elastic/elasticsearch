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
import org.junit.After;

import static org.hamcrest.Matchers.containsString;

/**
 * Verifies that {@link EsqlPlugin#GROK_WATCHDOG_MAX_EXECUTION_TIME} is honored at execution time.
 * The setting is {@code NodeScope} (every node reads its own value when it builds the GROK matcher
 * used against real data, see {@code LocalExecutionPlanner#planGrok} — no need to carry it in the
 * ES|QL wire format) and {@code Dynamic} (it can be changed via the cluster settings API without a
 * node restart, since {@code ComputeService} re-resolves it from the live {@code ClusterSettings}
 * for every query).
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class GrokWatchdogIT extends AbstractEsqlIntegTestCase {

    @After
    public void resetWatchdogSetting() {
        updateClusterSettings(Settings.builder().putNull(EsqlPlugin.GROK_WATCHDOG_MAX_EXECUTION_TIME.getKey()));
    }

    public void testCatastrophicBacktrackingPatternIsInterruptedAfterLiveSettingUpdate() {
        prepareIndex("test").setId("1").setSource("message", "a".repeat(30) + "X").get();
        refresh("test");

        // No restart: the setting is dynamic, so this update must take effect on the very next query.
        updateClusterSettings(Settings.builder().put(EsqlPlugin.GROK_WATCHDOG_MAX_EXECUTION_TIME.getKey(), TimeValue.timeValueMillis(1)));

        Exception e = expectThrows(Exception.class, () -> run("FROM test | GROK message \"(?<a>a+)+b\" | KEEP a"));
        assertThat(ExceptionsHelper.stackTrace(e), containsString("interrupted"));
    }
}
