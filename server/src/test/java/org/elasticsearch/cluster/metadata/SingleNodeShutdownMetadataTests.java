/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.test.ESTestCase;

public class SingleNodeShutdownMetadataTests extends ESTestCase {
    public void testStatusComination() {
        SingleNodeShutdownMetadata.Status status;

        status = SingleNodeShutdownMetadata.Status.combine(
            SingleNodeShutdownMetadata.Status.NOT_STARTED,
            SingleNodeShutdownMetadata.Status.IN_PROGRESS,
            SingleNodeShutdownMetadata.Status.STALLED
        );
        assertEquals(status, SingleNodeShutdownMetadata.Status.STALLED);

        status = SingleNodeShutdownMetadata.Status.combine(
            SingleNodeShutdownMetadata.Status.NOT_STARTED,
            SingleNodeShutdownMetadata.Status.IN_PROGRESS,
            SingleNodeShutdownMetadata.Status.NOT_STARTED
        );
        assertEquals(status, SingleNodeShutdownMetadata.Status.IN_PROGRESS);

        status = SingleNodeShutdownMetadata.Status.combine(
            SingleNodeShutdownMetadata.Status.NOT_STARTED,
            SingleNodeShutdownMetadata.Status.NOT_STARTED,
            SingleNodeShutdownMetadata.Status.NOT_STARTED
        );
        assertEquals(status, SingleNodeShutdownMetadata.Status.NOT_STARTED);

        status = SingleNodeShutdownMetadata.Status.combine(
            SingleNodeShutdownMetadata.Status.IN_PROGRESS,
            SingleNodeShutdownMetadata.Status.IN_PROGRESS,
            SingleNodeShutdownMetadata.Status.COMPLETE
        );
        assertEquals(status, SingleNodeShutdownMetadata.Status.IN_PROGRESS);

        status = SingleNodeShutdownMetadata.Status.combine(
            SingleNodeShutdownMetadata.Status.COMPLETE,
            SingleNodeShutdownMetadata.Status.COMPLETE,
            SingleNodeShutdownMetadata.Status.COMPLETE
        );
        assertEquals(status, SingleNodeShutdownMetadata.Status.COMPLETE);

        status = SingleNodeShutdownMetadata.Status.combine();
        assertEquals(status, SingleNodeShutdownMetadata.Status.COMPLETE);
    }

    public void testBuilderFromExistingShutdownMetadata() {
        // SIGTERM
        {
            final var singleNodeShutdownMetadata = SingleNodeShutdownMetadata.builder()
                .setNodeId(randomUUID())
                .setNodeEphemeralId(randomUUID())
                .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                .setReason(randomIdentifier())
                .setStartedAtMillis(randomNonNegativeLong())
                .setNodeSeen(randomBoolean())
                .setGracePeriod(randomPositiveTimeValue())
                .build();

            final var newSingleNodeShutdownMetadata = SingleNodeShutdownMetadata.builder(singleNodeShutdownMetadata).build();
            assertEquals(singleNodeShutdownMetadata, newSingleNodeShutdownMetadata);
        }

        // RESTART
        {
            final var singleNodeShutdownMetadata = SingleNodeShutdownMetadata.builder()
                .setNodeId(randomUUID())
                .setNodeEphemeralId(randomUUID())
                .setType(SingleNodeShutdownMetadata.Type.RESTART)
                .setReason(randomIdentifier())
                .setStartedAtMillis(randomNonNegativeLong())
                .setNodeSeen(randomBoolean())
                .setAllocationDelay(randomPositiveTimeValue())
                .build();

            final var newSingleNodeShutdownMetadata = SingleNodeShutdownMetadata.builder(singleNodeShutdownMetadata).build();
            assertEquals(singleNodeShutdownMetadata, newSingleNodeShutdownMetadata);
        }

        // REPLACE
        {
            final var singleNodeShutdownMetadata = SingleNodeShutdownMetadata.builder()
                .setNodeId(randomUUID())
                .setNodeEphemeralId(randomUUID())
                .setType(SingleNodeShutdownMetadata.Type.REPLACE)
                .setReason(randomIdentifier())
                .setStartedAtMillis(randomNonNegativeLong())
                .setNodeSeen(randomBoolean())
                .setTargetNodeName(randomIdentifier("node"))
                .build();

            final var newSingleNodeShutdownMetadata = SingleNodeShutdownMetadata.builder(singleNodeShutdownMetadata).build();
            assertEquals(singleNodeShutdownMetadata, newSingleNodeShutdownMetadata);
        }

        // REMOVE
        {
            final var singleNodeShutdownMetadata = SingleNodeShutdownMetadata.builder()
                .setNodeId(randomUUID())
                .setNodeEphemeralId(randomUUID())
                .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                .setReason(randomIdentifier())
                .setStartedAtMillis(randomNonNegativeLong())
                .setNodeSeen(randomBoolean())
                .build();

            final var newSingleNodeShutdownMetadata = SingleNodeShutdownMetadata.builder(singleNodeShutdownMetadata).build();
            assertEquals(singleNodeShutdownMetadata, newSingleNodeShutdownMetadata);
        }
    }
}
