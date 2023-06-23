/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService.CoordinationDiagnosticsDetails;
import static org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService.CoordinationDiagnosticsResult;
import static org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService.CoordinationDiagnosticsStatus;

public class CoordinationDiagnosticsActionTests extends ESTestCase {

    public void testSerialization() {
        DiscoveryNode node1 = DiscoveryNodeUtils.create("node1", UUID.randomUUID().toString());
        DiscoveryNode node2 = DiscoveryNodeUtils.create("node2", UUID.randomUUID().toString());
        CoordinationDiagnosticsDetails details = new CoordinationDiagnosticsDetails(
            node1,
            List.of(node1, node2),
            randomAlphaOfLengthBetween(0, 30),
            randomAlphaOfLengthBetween(0, 30),
            Map.of(randomAlphaOfLength(20), randomAlphaOfLengthBetween(0, 30))
        );
        CoordinationDiagnosticsResult result = new CoordinationDiagnosticsResult(
            randomFrom(CoordinationDiagnosticsStatus.values()),
            randomAlphaOfLength(100),
            details
        );
        CoordinationDiagnosticsAction.Response response = new CoordinationDiagnosticsAction.Response(result);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            response,
            history -> copyWriteable(history, writableRegistry(), CoordinationDiagnosticsAction.Response::new),
            this::mutateResponse
        );

        CoordinationDiagnosticsAction.Request request = new CoordinationDiagnosticsAction.Request(randomBoolean());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            request,
            history -> copyWriteable(history, writableRegistry(), CoordinationDiagnosticsAction.Request::new),
            this::mutateRequest
        );
    }

    private CoordinationDiagnosticsAction.Request mutateRequest(CoordinationDiagnosticsAction.Request originalRequest) {
        return new CoordinationDiagnosticsAction.Request(originalRequest.explain == false);
    }

    private CoordinationDiagnosticsAction.Response mutateResponse(CoordinationDiagnosticsAction.Response originalResponse) {
        CoordinationDiagnosticsResult originalResult = originalResponse.getCoordinationDiagnosticsResult();
        return new CoordinationDiagnosticsAction.Response(
            new CoordinationDiagnosticsResult(originalResult.status(), randomAlphaOfLength(100), originalResult.details())
        );
    }
}
