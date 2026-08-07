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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.COMPLETE;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.NOT_STARTED;
import static org.hamcrest.Matchers.equalTo;

public class ShutdownPersistentTasksStatusTests extends AbstractWireSerializingTestCase<ShutdownPersistentTasksStatus> {

    @Override
    protected Writeable.Reader<ShutdownPersistentTasksStatus> instanceReader() {
        return ShutdownPersistentTasksStatus::new;
    }

    @Override
    protected ShutdownPersistentTasksStatus createTestInstance() {
        if (randomBoolean()) {
            return ShutdownPersistentTasksStatus.notStarted();
        } else {
            int persistentTasksRemaining = randomIntBetween(0, 10);
            int autoReassignableTasksRemaining = randomIntBetween(0, persistentTasksRemaining);
            return ShutdownPersistentTasksStatus.fromRemainingTasks(persistentTasksRemaining, autoReassignableTasksRemaining);
        }
    }

    @Override
    protected ShutdownPersistentTasksStatus mutateInstance(ShutdownPersistentTasksStatus instance) throws IOException {
        return switch (randomInt(3)) {
            case 0 -> instance.getStatus() == COMPLETE
                ? ShutdownPersistentTasksStatus.fromRemainingTasks(instance.getPersistentTasksRemaining() + 1, 1)
                : ShutdownPersistentTasksStatus.fromRemainingTasks(instance.getPersistentTasksRemaining(), 0);
            case 1 -> instance.getStatus() == NOT_STARTED
                ? ShutdownPersistentTasksStatus.fromRemainingTasks(0, 0)
                : ShutdownPersistentTasksStatus.fromRemainingTasks(
                    instance.getPersistentTasksRemaining() + randomIntBetween(1, 10),
                    instance.getAutoReassignableTasksRemaining()
                );
            case 2 -> ShutdownPersistentTasksStatus.fromRemainingTasks(
                instance.getPersistentTasksRemaining() + randomIntBetween(5, 10),
                instance.getAutoReassignableTasksRemaining() + randomIntBetween(1, 5)
            );
            default -> instance.getStatus() == NOT_STARTED
                ? ShutdownPersistentTasksStatus.fromRemainingTasks(0, 0)
                : ShutdownPersistentTasksStatus.notStarted();
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
        assertThat(deserialized.getAutoReassignableTasksRemaining(), equalTo(0));
    }
}
