/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.application.connector.ConnectorTestUtils;
import org.elasticsearch.xpack.application.connector.syncjob.action.DeleteConnectorSyncJobAction;
import org.elasticsearch.xpack.application.connector.syncjob.action.PostConnectorSyncJobAction;

import java.time.Instant;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInstantBetween;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomMap;

public class ConnectorSyncJobTestUtils {

    public static ConnectorSyncJob getRandomConnectorSyncJob() {
        Instant lowerBoundInstant = Instant.ofEpochSecond(0L);
        Instant upperBoundInstant = Instant.ofEpochSecond(3000000000L);

        return new ConnectorSyncJob.Builder().setCancellationRequestedAt(
            randomFrom(new Instant[] { null, randomInstantBetween(lowerBoundInstant, upperBoundInstant) })
        )
            .setCanceledAt(randomFrom(new Instant[] { null, randomInstantBetween(lowerBoundInstant, upperBoundInstant) }))
            .setCompletedAt(randomFrom(new Instant[] { null, randomInstantBetween(lowerBoundInstant, upperBoundInstant) }))
            .setConnector(ConnectorTestUtils.getRandomSyncJobConnectorInfo())
            .setCreatedAt(randomInstantBetween(lowerBoundInstant, upperBoundInstant))
            .setDeletedDocumentCount(randomLong())
            .setError(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setId(randomAlphaOfLength(10))
            .setIndexedDocumentCount(randomLong())
            .setIndexedDocumentVolume(randomLong())
            .setJobType(getRandomConnectorJobType())
            .setLastSeen(randomFrom(new Instant[] { null, randomInstantBetween(lowerBoundInstant, upperBoundInstant) }))
            .setMetadata(
                randomMap(
                    0,
                    10,
                    () -> new Tuple<>(randomAlphaOfLength(10), randomFrom(new Object[] { null, randomAlphaOfLength(10), randomLong() }))
                )
            )
            .setStartedAt(randomFrom(new Instant[] { null, randomInstantBetween(lowerBoundInstant, upperBoundInstant) }))
            .setStatus(ConnectorTestUtils.getRandomSyncStatus())
            .setTotalDocumentCount(randomLong())
            .setTriggerMethod(getRandomConnectorSyncJobTriggerMethod())
            .setWorkerHostname(randomAlphaOfLength(10))
            .build();
    }

    public static ConnectorSyncJobTriggerMethod getRandomConnectorSyncJobTriggerMethod() {
        ConnectorSyncJobTriggerMethod[] values = ConnectorSyncJobTriggerMethod.values();
        return values[randomInt(values.length - 1)];
    }

    public static ConnectorSyncJobType getRandomConnectorJobType() {
        ConnectorSyncJobType[] values = ConnectorSyncJobType.values();
        return values[randomInt(values.length - 1)];
    }

    public static PostConnectorSyncJobAction.Request getRandomPostConnectorSyncJobActionRequest() {
        return new PostConnectorSyncJobAction.Request(
            randomAlphaOfLengthBetween(5, 15),
            randomFrom(ConnectorSyncJobType.values()),
            randomFrom(ConnectorSyncJobTriggerMethod.values())
        );
    }

    public static DeleteConnectorSyncJobAction.Request getRandomDeleteConnectorSyncJobActionRequest() {
        return new DeleteConnectorSyncJobAction.Request(randomAlphaOfLengthBetween(5, 15));
    }

    public static PostConnectorSyncJobAction.Request getRandomPostConnectorSyncJobActionRequest(String connectorId) {
        return new PostConnectorSyncJobAction.Request(
            connectorId,
            randomFrom(ConnectorSyncJobType.values()),
            randomFrom(ConnectorSyncJobTriggerMethod.values())
        );
    }

    public static PostConnectorSyncJobAction.Response getRandomPostConnectorSyncJobActionResponse() {
        return new PostConnectorSyncJobAction.Response(randomAlphaOfLength(10));
    }
}
