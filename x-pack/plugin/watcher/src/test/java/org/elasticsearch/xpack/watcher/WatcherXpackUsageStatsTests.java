/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.watcher.WatcherFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.is;

public class WatcherXpackUsageStatsTests extends AbstractWatcherIntegrationTestCase {

    // as these tests use three data nodes, those watches will be across two of those
    // nodes due to having two watcher shards, so that we can be sure that the count
    // was merged
    public void testWatcherUsageStatsTests() {
        long watchCount = randomLongBetween(5, 20);
        for (int i = 0; i < watchCount; i++) {
            new PutWatchRequestBuilder(client(), "_id" + i).setSource(watchBuilder()
                    .trigger(schedule(cron("0/5 * * * * ? 2050")))
                    .input(simpleInput())
                    .addAction("_id", loggingAction("whatever " + i)))
                    .get();
        }

        XPackUsageRequest request = new XPackUsageRequest();
        XPackUsageResponse usageResponse = client().execute(XPackUsageAction.INSTANCE, request).actionGet();
        Optional<XPackFeatureSet.Usage> usage = usageResponse.getUsages().stream()
                .filter(u -> u instanceof WatcherFeatureSetUsage)
                .findFirst();
        assertThat(usage.isPresent(), is(true));
        WatcherFeatureSetUsage featureSetUsage = (WatcherFeatureSetUsage) usage.get();

        long activeWatchCount = (long) ((Map) featureSetUsage.stats().get("count")).get("active");
        assertThat(activeWatchCount, is(watchCount));
    }

}
