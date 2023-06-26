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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponseTests.randomIndexResponse;
import static org.hamcrest.Matchers.equalTo;

public class FieldCapabilitiesNodeResponseTests extends AbstractWireSerializingTestCase<FieldCapabilitiesNodeResponse> {

    @Override
    protected FieldCapabilitiesNodeResponse createTestInstance() {
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        int numResponse = randomIntBetween(0, 10);
        for (int i = 0; i < numResponse; i++) {
            responses.add(randomIndexResponse());
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
            case 0:
                newResponses.add(randomIndexResponse());
                break;
            case 1:
                int toRemove = randomInt(newResponses.size() - 1);
                newResponses.remove(toRemove);
                break;
            case 2:
                int toReplace = randomInt(newResponses.size() - 1);
                newResponses.set(toReplace, randomIndexResponse());
                break;
        }
        return new FieldCapabilitiesNodeResponse(newResponses, Collections.emptyMap(), response.getUnmatchedShardIds());
    }

    public void testSharedFieldsBetweenIndexResponses() throws IOException {
        List<FieldCapabilitiesIndexResponse> indexResponses = new ArrayList<>();
        FieldCapabilitiesIndexResponse firstResponse = randomIndexResponse();
        indexResponses.add(firstResponse);
        int extraResponses = randomIntBetween(2, 10);
        for (int i = 0; i < extraResponses; i++) {
            final List<IndexFieldCapabilities> fields = new ArrayList<>();
            for (IndexFieldCapabilities f : firstResponse.getFields()) {
                if (frequently()) {
                    if (randomBoolean()) {
                        fields.add(f);
                    } else {
                        fields.add(copyWriteable(f, getNamedWriteableRegistry(), IndexFieldCapabilities::new));
                    }
                }
            }
            FieldCapabilitiesIndexResponse indexResponse = new FieldCapabilitiesIndexResponse(
                randomAlphaOfLength(100),
                fields,
                firstResponse.canMatch()
            );
            indexResponses.add(indexResponse);
        }
        int numUnmatched = randomIntBetween(0, 3);
        Set<ShardId> shardIds = new HashSet<>();
        for (int i = 0; i < numUnmatched; i++) {
            shardIds.add(new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), between(0, 10)));
        }
        FieldCapabilitiesNodeResponse originalResp = new FieldCapabilitiesNodeResponse(indexResponses, Collections.emptyMap(), shardIds);
        FieldCapabilitiesNodeResponse serializedResp = copyInstance(originalResp);
        assertThat(serializedResp, equalTo(originalResp));
        final Map<String, IndexFieldCapabilities> sharedFields = new HashMap<>();
        assertThat(serializedResp.getIndexResponses().get(0), equalTo(firstResponse));
        for (IndexFieldCapabilities f : serializedResp.getIndexResponses().get(0).getFields()) {
            sharedFields.putIfAbsent(f.getName(), f);
        }
        for (int i = 1; i < serializedResp.getIndexResponses().size(); i++) {
            for (IndexFieldCapabilities f : serializedResp.getIndexResponses().get(i).getFields()) {
                assertSame(sharedFields.get(f.getName()), f);
            }
        }
    }
}
