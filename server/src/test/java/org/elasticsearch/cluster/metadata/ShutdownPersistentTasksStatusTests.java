/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.COMPLETE;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.IN_PROGRESS;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.NOT_STARTED;
import static org.hamcrest.Matchers.equalTo;

public class ShutdownPersistentTasksStatusTests extends AbstractWireSerializingTestCase<ShutdownPersistentTasksStatus> {

    @Override
    protected Writeable.Reader<ShutdownPersistentTasksStatus> instanceReader() {
        return ShutdownPersistentTasksStatus::new;
    }

    @Override
    protected ShutdownPersistentTasksStatus createTestInstance() {
        int persistentTasksRemaining = randomIntBetween(0, 10);
        int autoReassignedTasksRemaining = randomIntBetween(0, persistentTasksRemaining);
        Status status = autoReassignedTasksRemaining == 0 ? COMPLETE : IN_PROGRESS;
        return new ShutdownPersistentTasksStatus(status, persistentTasksRemaining, autoReassignedTasksRemaining);
    }

    @Override
    protected ShutdownPersistentTasksStatus mutateInstance(ShutdownPersistentTasksStatus instance) throws IOException {
        return switch (randomInt(3)) {
            case 0 -> instance.getStatus() == COMPLETE
                ? new ShutdownPersistentTasksStatus(IN_PROGRESS, instance.getPersistentTasksRemaining() + 1, 1)
                : new ShutdownPersistentTasksStatus(COMPLETE, instance.getPersistentTasksRemaining(), 0);
            case 1 -> instance.getStatus() == NOT_STARTED
                ? new ShutdownPersistentTasksStatus(COMPLETE, 0, 0)
                : new ShutdownPersistentTasksStatus(
                    instance.getStatus(),
                    instance.getPersistentTasksRemaining() + randomIntBetween(1, 10),
                    instance.getAutoReassignedTasksRemaining()
                );
            case 2 -> new ShutdownPersistentTasksStatus(
                IN_PROGRESS,
                instance.getPersistentTasksRemaining() + randomIntBetween(5, 10),
                instance.getAutoReassignedTasksRemaining() + randomIntBetween(1, 5)
            );
            default -> new ShutdownPersistentTasksStatus(NOT_STARTED, 0, 0);
        };
    }

    /**
     * Verifies wire compatibility with transport versions older than
     * {@link ShutdownPersistentTasksStatus#SHUTDOWN_PERSISTENT_TASKS_STATUS}.
     */
    public void testBackwardCompatibleSerialization() throws IOException {
        final TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(
            ShutdownPersistentTasksStatus.SHUTDOWN_PERSISTENT_TASKS_STATUS
        );
        final ShutdownPersistentTasksStatus instance = createTestInstance();
        final ShutdownPersistentTasksStatus deserialized = copyInstance(instance, oldVersion);

        assertThat(deserialized.getStatus(), equalTo(COMPLETE));
        assertThat(deserialized.getPersistentTasksRemaining(), equalTo(0));
        assertThat(deserialized.getAutoReassignedTasksRemaining(), equalTo(0));
    }
}
