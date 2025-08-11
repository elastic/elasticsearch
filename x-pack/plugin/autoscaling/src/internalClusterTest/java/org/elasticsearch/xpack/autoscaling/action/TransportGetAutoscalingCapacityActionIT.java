/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.core.Set;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.autoscaling.AutoscalingIntegTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.hamcrest.Matchers;

import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

@TestLogging(
    value = "org.elasticsearch.xpack.autoscaling.action.TransportGetAutoscalingCapacityAction:debug",
    reason = "to ensure we log autoscaling capacity response on DEBUG level"
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TransportGetAutoscalingCapacityActionIT extends AutoscalingIntegTestCase {

    public void testCurrentCapacity() throws Exception {
        assertThat(capacity().results().keySet(), Matchers.empty());
        long memory = OsProbe.getInstance().getTotalPhysicalMemorySize();
        long storage = internalCluster().getInstance(NodeEnvironment.class).dataPaths()[0].fileStore.getTotalSpace();
        assertThat(memory, greaterThan(0L));
        assertThat(storage, greaterThan(0L));
        putAutoscalingPolicy("test");
        assertCurrentCapacity(0, 0, 0);

        int nodes = between(1, 5);
        internalCluster().startDataOnlyNodes(nodes);

        assertBusy(() -> { assertCurrentCapacity(memory, storage, nodes); });
    }

    public void assertCurrentCapacity(long memory, long storage, int nodes) throws IllegalAccessException {
        Logger subjectLogger = LogManager.getLogger(TransportGetAutoscalingCapacityAction.class);

        MockLogAppender appender = new MockLogAppender();
        appender.start();
        appender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "autoscaling capacity response message with " + storage,
                TransportGetAutoscalingCapacityAction.class.getName(),
                Level.DEBUG,
                "autoscaling capacity response [*\"policies\"*\"test\"*\"current_capacity\"*\"storage\":"
                    + storage
                    + "*\"deciders\""
                    + "*\"reactive_storage\""
                    + "*\"reason_summary\"*\"reason_details\"*]"
            )
        );
        Loggers.addAppender(subjectLogger, appender);
        try {
            GetAutoscalingCapacityAction.Response capacity = capacity();
            AutoscalingCapacity currentCapacity = capacity.results().get("test").currentCapacity();
            assertThat(currentCapacity.node().memory().getBytes(), Matchers.equalTo(memory));
            assertThat(currentCapacity.total().memory().getBytes(), Matchers.equalTo(memory * nodes));
            assertThat(currentCapacity.node().storage().getBytes(), Matchers.equalTo(storage));
            assertThat(currentCapacity.total().storage().getBytes(), Matchers.equalTo(storage * nodes));
            appender.assertAllExpectationsMatched();
        } finally {
            appender.stop();
            Loggers.removeAppender(subjectLogger, appender);
        }
    }

    public GetAutoscalingCapacityAction.Response capacity() {
        GetAutoscalingCapacityAction.Request request = new GetAutoscalingCapacityAction.Request();
        GetAutoscalingCapacityAction.Response response = client().execute(GetAutoscalingCapacityAction.INSTANCE, request).actionGet();
        return response;
    }

    private void putAutoscalingPolicy(String policyName) {
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            policyName,
            new TreeSet<>(Set.of("data")),
            new TreeMap<>()
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());
    }

}
