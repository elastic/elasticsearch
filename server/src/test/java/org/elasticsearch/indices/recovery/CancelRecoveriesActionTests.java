/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class CancelRecoveriesActionTests extends ESTestCase {

    public void testRequestSerialization() throws Exception {
        TransportVersion serializationVersion = TransportVersionUtils.randomVersion();
        long clusterStateVersion = randomNonNegativeLong();

        List<CancelRecoveriesAction.ShardRecoveryCancellation> cancellations = List.of(
            new CancelRecoveriesAction.ShardRecoveryCancellation(
                new ShardId("test", UUIDs.randomBase64UUID(), 0),
                UUIDs.randomBase64UUID(),
                randomBoolean()
            ),
            new CancelRecoveriesAction.ShardRecoveryCancellation(
                new ShardId("test", UUIDs.randomBase64UUID(), 1),
                UUIDs.randomBase64UUID(),
                randomBoolean()
            )
        );

        final CancelRecoveriesAction.Request outRequest = new CancelRecoveriesAction.Request(clusterStateVersion, cancellations);

        final CancelRecoveriesAction.Request inRequest = copyWriteable(
            outRequest,
            writableRegistry(),
            CancelRecoveriesAction.Request::new,
            serializationVersion
        );

        assertThat(inRequest.clusterStateVersion(), equalTo(outRequest.clusterStateVersion()));
        assertThat(inRequest.cancellations().size(), equalTo(outRequest.cancellations().size()));

        for (int i = 0; i < inRequest.cancellations().size(); i++) {
            CancelRecoveriesAction.ShardRecoveryCancellation inCancellation = inRequest.cancellations().get(i);
            CancelRecoveriesAction.ShardRecoveryCancellation outCancellation = outRequest.cancellations().get(i);

            assertThat(inCancellation.shardId(), equalTo(outCancellation.shardId()));
            assertThat(inCancellation.allocationId(), equalTo(outCancellation.allocationId()));
            assertThat(inCancellation.cancelIfStarted(), equalTo(outCancellation.cancelIfStarted()));
        }
    }

    public void testResponseSerialization() throws Exception {
        final var serializationVersion = TransportVersionUtils.randomVersion();
        final var cancelledInQueue = Set.copyOf(randomList(0, 5, UUIDs::randomBase64UUID));
        final var outResponse = new CancelRecoveriesAction.Response(cancelledInQueue);
        final var inResponse = copyWriteable(outResponse, writableRegistry(), CancelRecoveriesAction.Response::new, serializationVersion);
        assertThat(inResponse.cancelledInQueue(), equalTo(outResponse.cancelledInQueue()));
    }
}
