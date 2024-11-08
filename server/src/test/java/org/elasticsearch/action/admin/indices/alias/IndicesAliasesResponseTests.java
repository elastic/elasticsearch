/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.alias.RandomAliasActionsGenerator;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IndicesAliasesResponseTests extends AbstractWireSerializingTestCase<IndicesAliasesResponse> {
    public void testMixedModeSerialization() throws IOException {

        // AcknowledgedResponse to IndicesAliasesResponse
        // in version before TransportVersions.ALIAS_ACTION_RESULTS
        {
            var ack = AcknowledgedResponse.of(randomBoolean());
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                ack.writeTo(output);
                try (StreamInput in = output.bytes().streamInput()) {
                    in.setTransportVersion(TransportVersions.V_8_12_0);

                    var indicesAliasesResponse = new IndicesAliasesResponse(in);

                    assertEquals(ack.isAcknowledged(), indicesAliasesResponse.isAcknowledged());
                    assertTrue(indicesAliasesResponse.getActionResults().isEmpty());
                    assertFalse(indicesAliasesResponse.hasErrors());
                }
            }
        }

        // IndicesAliasesResponse to AcknowledgedResponse
        // out version before TransportVersions.ALIAS_ACTION_RESULTS
        {
            var indicesAliasesResponse = randomIndicesAliasesResponse();
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.setTransportVersion(TransportVersions.V_8_12_0);

                indicesAliasesResponse.writeTo(output);
                try (StreamInput in = output.bytes().streamInput()) {
                    var ack = AcknowledgedResponse.readFrom(in);
                    assertEquals(ack.isAcknowledged(), indicesAliasesResponse.isAcknowledged());
                }
            }
        }
    }

    @Override
    protected Writeable.Reader<IndicesAliasesResponse> instanceReader() {
        return IndicesAliasesResponse::new;
    }

    @Override
    protected IndicesAliasesResponse createTestInstance() {
        return randomIndicesAliasesResponse();
    }

    private static IndicesAliasesResponse randomIndicesAliasesResponse() {
        int numActions = between(0, 5);
        List<IndicesAliasesResponse.AliasActionResult> results = new ArrayList<>();
        for (int i = 0; i < numActions; ++i) {
            results.add(randomIndicesAliasesResult());
        }
        return new IndicesAliasesResponse(randomBoolean(), randomBoolean(), results);
    }

    @Override
    protected IndicesAliasesResponse mutateInstance(IndicesAliasesResponse instance) throws IOException {
        switch (between(0, 2)) {
            case 0: {
                boolean acknowledged = instance.isAcknowledged() == false;
                return new IndicesAliasesResponse(acknowledged, instance.hasErrors(), instance.getActionResults());
            }
            case 1: {
                boolean errors = instance.hasErrors() == false;
                return new IndicesAliasesResponse(instance.isAcknowledged(), errors, instance.getActionResults());
            }
            default: {
                var results = new ArrayList<>(instance.getActionResults());
                if (results.isEmpty()) {
                    results.add(randomIndicesAliasesResult());
                } else {
                    results.remove(between(0, results.size() - 1));
                }
                return new IndicesAliasesResponse(instance.isAcknowledged(), instance.hasErrors(), results);
            }
        }
    }

    private static IndicesAliasesResponse.AliasActionResult randomIndicesAliasesResult() {
        var action = RandomAliasActionsGenerator.randomAliasAction();
        var indices = Arrays.asList(generateRandomStringArray(10, 5, false, false));
        return IndicesAliasesResponse.AliasActionResult.build(indices, action, randomIntBetween(0, 3));
    }
}
