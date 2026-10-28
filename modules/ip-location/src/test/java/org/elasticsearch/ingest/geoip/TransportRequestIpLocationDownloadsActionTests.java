/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.ingest.geoip.RequestIpLocationDownloadsAction.Request.Operation;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.sameInstance;

public class TransportRequestIpLocationDownloadsActionTests extends ESTestCase {

    /**
     * The master-side cluster state task is where failures of consumer register/unregister requests are
     * logged (the caller side purposely uses {@link ActionListener#noop()} to avoid double-logging). This
     * test verifies the WARN includes enough context for troubleshooting and that the original failure is
     * propagated to the response listener.
     */
    public void testOnFailureLogsWarningAndPropagatesToListener() {
        ProjectId projectId = randomProjectIdOrDefault();
        IpLocationConsumer consumer = randomFrom(IpLocationConsumer.values());
        Operation operation = randomFrom(Operation.values());
        RuntimeException failure = new RuntimeException("simulated cluster state update failure");

        AtomicReference<Exception> listenerFailure = new AtomicReference<>();
        ActionListener<AcknowledgedResponse> listener = ActionListener.wrap(
            response -> fail("unexpected success: " + response),
            listenerFailure::set
        );

        TransportRequestIpLocationDownloadsAction.UpdateConsumersTask task =
            new TransportRequestIpLocationDownloadsAction.UpdateConsumersTask(projectId, listener, consumer, operation);

        MockLog.assertThatLogger(
            () -> task.onFailure(failure),
            TransportRequestIpLocationDownloadsAction.class,
            new MockLog.SeenEventExpectation(
                "update-consumers failure warning",
                TransportRequestIpLocationDownloadsAction.class.getCanonicalName(),
                Level.WARN,
                "failed to apply ip-location download consumer update for project ["
                    + projectId
                    + "]: "
                    + operation.label()
                    + " consumer ["
                    + consumer
                    + "]"
            )
        );

        assertThat(listenerFailure.get(), sameInstance(failure));
    }
}
