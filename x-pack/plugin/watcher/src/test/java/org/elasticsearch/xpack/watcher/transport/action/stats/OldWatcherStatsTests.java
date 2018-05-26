/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.action.stats;

import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.stats.OldWatcherStatsAction;
import org.elasticsearch.xpack.watcher.transport.actions.stats.OldWatcherStatsRequest;
import org.elasticsearch.xpack.watcher.transport.actions.stats.OldWatcherStatsResponse;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class OldWatcherStatsTests extends AbstractWatcherIntegrationTestCase {

    // these stats are not exposed in the watcherclient as they are only needed for the rolling upgrade
    public void testPre6xWatcherStats() throws Exception {
        OldWatcherStatsResponse response = client().execute(OldWatcherStatsAction.INSTANCE, new OldWatcherStatsRequest()).actionGet();
        assertThat(response.getThreadPoolMaxSize(), is(greaterThanOrEqualTo(0L)));
        assertThat(response.getThreadPoolQueueSize(), is(greaterThanOrEqualTo(0L)));
        assertThat(response.getWatchesCount(), is(greaterThanOrEqualTo(0L)));
        assertThat(response.getWatcherMetaData().manuallyStopped(), is(false));
    }
}
