/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;
import org.elasticsearch.xpack.application.connector.ConnectorTestUtils;
import org.elasticsearch.xpack.application.connector.syncjob.action.CancelConnectorSyncJobAction;
import org.elasticsearch.xpack.application.connector.syncjob.action.CheckInConnectorSyncJobAction;
import org.elasticsearch.xpack.application.connector.syncjob.action.ClaimConnectorSyncJobAction;
import org.elasticsearch.xpack.application.connector.syncjob.action.DeleteConnectorSyncJobAction;
import org.elasticsearch.xpack.application.connector.syncjob.action.GetConnectorSyncJobAction;
import org.elasticsearch.xpack.application.connector.syncjob.action.ListConnectorSyncJobsAction;
import org.elasticsearch.xpack.application.connector.syncjob.action.PostConnectorSyncJobAction;
import org.elasticsearch.xpack.application.connector.syncjob.action.UpdateConnectorSyncJobErrorAction;
import org.elasticsearch.xpack.application.connector.syncjob.action.UpdateConnectorSyncJobIngestionStatsAction;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInstantBetween;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomMap;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeInt;

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
            .setDeletedDocumentCount(randomNonNegativeInt())
            .setError(randomFrom(new String[] { null, randomAlphaOfLength(10) }))
            .setId(randomAlphaOfLength(10))
            .setIndexedDocumentCount(randomNonNegativeInt())
            .setIndexedDocumentVolume(randomNonNegativeInt())
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
            .setTotalDocumentCount(randomNonNegativeInt())
            .setTriggerMethod(getRandomConnectorSyncJobTriggerMethod())
            .setWorkerHostname(randomAlphaOfLength(10))
            .build();
    }

    private static BytesReference convertSyncJobToBytesReference(ConnectorSyncJob syncJob) {
        try {
            return XContentHelper.toXContent((builder, params) -> {
                syncJob.toInnerXContent(builder, params);
                return builder;
            }, XContentType.JSON, null, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, Object> convertSyncJobToGenericMap(ConnectorSyncJob syncJob) {
        return XContentHelper.convertToMap(convertSyncJobToBytesReference(syncJob), true, XContentType.JSON).v2();
    }

    public static ConnectorSyncJobSearchResult getRandomSyncJobSearchResult() {
        ConnectorSyncJob syncJob = getRandomConnectorSyncJob();

        return new ConnectorSyncJobSearchResult.Builder().setId(randomAlphaOfLength(10))
            .setResultMap(convertSyncJobToGenericMap(syncJob))
            .setResultBytes(convertSyncJobToBytesReference(syncJob))
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

    public static PostConnectorSyncJobAction.Request getRandomPostConnectorSyncJobActionRequest(
        String connectorId,
        ConnectorSyncJobType jobType
    ) {
        return new PostConnectorSyncJobAction.Request(connectorId, jobType, randomFrom(ConnectorSyncJobTriggerMethod.values()));
    }

    public static PostConnectorSyncJobAction.Response getRandomPostConnectorSyncJobActionResponse() {
        return new PostConnectorSyncJobAction.Response(randomAlphaOfLength(10));
    }

    public static CancelConnectorSyncJobAction.Request getRandomCancelConnectorSyncJobActionRequest() {
        return new CancelConnectorSyncJobAction.Request(randomAlphaOfLength(10));
    }

    public static CheckInConnectorSyncJobAction.Request getRandomCheckInConnectorSyncJobActionRequest() {
        return new CheckInConnectorSyncJobAction.Request(randomAlphaOfLength(10));
    }

    public static UpdateConnectorSyncJobErrorAction.Request getRandomUpdateConnectorSyncJobErrorActionRequest() {
        return new UpdateConnectorSyncJobErrorAction.Request(randomAlphaOfLength(10), randomAlphaOfLengthBetween(5, 100));
    }

    public static UpdateConnectorSyncJobIngestionStatsAction.Request getRandomUpdateConnectorSyncJobIngestionStatsActionRequest() {
        Instant lowerBoundInstant = Instant.ofEpochSecond(0L);
        Instant upperBoundInstant = Instant.ofEpochSecond(3000000000L);

        return new UpdateConnectorSyncJobIngestionStatsAction.Request(
            randomAlphaOfLength(10),
            (long) randomNonNegativeInt(),
            (long) randomNonNegativeInt(),
            (long) randomNonNegativeInt(),
            (long) randomNonNegativeInt(),
            randomInstantBetween(lowerBoundInstant, upperBoundInstant),
            randomMap(2, 3, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4)))
        );
    }

    public static UpdateConnectorSyncJobIngestionStatsAction.Request getRandomUpdateConnectorSyncJobIngestionStatsActionRequest(
        String syncJobId
    ) {
        Instant lowerBoundInstant = Instant.ofEpochSecond(0L);
        Instant upperBoundInstant = Instant.ofEpochSecond(3000000000L);

        return new UpdateConnectorSyncJobIngestionStatsAction.Request(
            syncJobId,
            (long) randomNonNegativeInt(),
            (long) randomNonNegativeInt(),
            (long) randomNonNegativeInt(),
            (long) randomNonNegativeInt(),
            randomInstantBetween(lowerBoundInstant, upperBoundInstant),
            randomMap(2, 3, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4)))
        );
    }

    public static GetConnectorSyncJobAction.Request getRandomGetConnectorSyncJobRequest() {
        return new GetConnectorSyncJobAction.Request(randomAlphaOfLength(10));
    }

    public static GetConnectorSyncJobAction.Response getRandomGetConnectorSyncJobResponse() {
        return new GetConnectorSyncJobAction.Response(getRandomSyncJobSearchResult());
    }

    public static ListConnectorSyncJobsAction.Request getRandomListConnectorSyncJobsActionRequest() {
        return new ListConnectorSyncJobsAction.Request(
            EnterpriseSearchModuleTestUtils.randomPageParams(),
            randomAlphaOfLength(10),
            ConnectorTestUtils.getRandomSyncStatus(),
            Collections.singletonList(ConnectorTestUtils.getRandomSyncJobType())
        );
    }

    public static ClaimConnectorSyncJobAction.Request getRandomClaimConnectorSyncJobActionRequest() {
        return new ClaimConnectorSyncJobAction.Request(
            randomAlphaOfLength(10),
            randomAlphaOfLengthBetween(10, 100),
            randomBoolean() ? Map.of("test", "123") : null
        );
    }
}
