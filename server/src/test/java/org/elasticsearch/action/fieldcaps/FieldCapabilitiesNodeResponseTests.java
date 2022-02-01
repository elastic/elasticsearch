/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FieldCapabilitiesNodeResponseTests extends AbstractWireSerializingTestCase<FieldCapabilitiesNodeResponse> {

    @Override
    protected FieldCapabilitiesNodeResponse createTestInstance() {
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        int numResponse = randomIntBetween(0, 10);
        for (int i = 0; i < numResponse; i++) {
            responses.add(FieldCapabilitiesResponseTests.createRandomIndexResponse());
        }
        int numUnmatched = randomIntBetween(0, 3);
        Set<ShardId> shardIds = new HashSet<>();
        for (int i = 0; i < numUnmatched; i++) {
            shardIds.add(new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), between(0, 10)));
        }
        return new FieldCapabilitiesNodeResponse(responses, Collections.emptyMap(), shardIds);
    }

    @Override
    protected Writeable.Reader<FieldCapabilitiesNodeResponse> instanceReader() {
        return FieldCapabilitiesNodeResponse::new;
    }

    @Override
    protected FieldCapabilitiesNodeResponse mutateInstance(FieldCapabilitiesNodeResponse response) {
        List<FieldCapabilitiesIndexResponse> newResponses = new ArrayList<>(response.getIndexResponses());
        int mutation = response.getIndexResponses().isEmpty() ? 0 : randomIntBetween(0, 2);
        switch (mutation) {
            case 0 -> newResponses.add(FieldCapabilitiesResponseTests.createRandomIndexResponse());
            case 1 -> {
                int toRemove = randomInt(newResponses.size() - 1);
                newResponses.remove(toRemove);
            }
            case 2 -> {
                int toReplace = randomInt(newResponses.size() - 1);
                newResponses.set(toReplace, FieldCapabilitiesResponseTests.createRandomIndexResponse());
            }
        }
        return new FieldCapabilitiesNodeResponse(newResponses, Collections.emptyMap(), response.getUnmatchedShardIds());
    }
}
