/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiGetShardRequestTests extends AbstractWireSerializingTestCase<MultiGetShardRequest> {

    @Override
    protected MultiGetShardRequest createTestInstance() {
        return createTestInstance(randomBoolean());
    }

    @Override
    protected MultiGetShardRequest mutateInstance(MultiGetShardRequest instance) throws IOException {
        int mutationBranch = randomInt(6);
        var multiGetRequest = new MultiGetRequest().preference(instance.preference())
            .realtime(instance.realtime())
            .refresh(instance.refresh())
            .setForceSyntheticSource(instance.isForceSyntheticSource());
        switch (mutationBranch) {
            case 0 -> {
                var multiGetShardRequest = new MultiGetShardRequest(multiGetRequest, instance.index(), instance.shardId());
                multiGetShardRequest.locations = instance.locations;
                multiGetShardRequest.items = instance.items;
                multiGetShardRequest.preference(
                    randomValueOtherThan(instance.preference(), () -> randomAlphaOfLength(randomIntBetween(1, 10)))
                );
                return multiGetShardRequest;
            }
            case 1 -> {
                var multiGetShardRequest = new MultiGetShardRequest(multiGetRequest, instance.index(), instance.shardId());
                multiGetShardRequest.locations = instance.locations;
                multiGetShardRequest.items = instance.items;
                multiGetShardRequest.realtime(instance.realtime() ? false : true);
                return multiGetShardRequest;
            }
            case 2 -> {
                var multiGetShardRequest = new MultiGetShardRequest(multiGetRequest, instance.index(), instance.shardId());
                multiGetShardRequest.locations = instance.locations;
                multiGetShardRequest.items = instance.items;
                multiGetShardRequest.refresh(instance.refresh() ? false : true);
                return multiGetShardRequest;
            }
            case 3 -> {
                var multiGetShardRequest = new MultiGetShardRequest(multiGetRequest, instance.index(), instance.shardId());
                multiGetShardRequest.locations = instance.locations;
                multiGetShardRequest.items = instance.items;
                multiGetShardRequest.setForceSyntheticSource(instance.isForceSyntheticSource() ? false : true);
                return multiGetShardRequest;
            }
            case 4 -> {
                var multiGetShardRequest = new MultiGetShardRequest(
                    multiGetRequest,
                    instance.index(),
                    randomValueOtherThan(instance.shardId(), () -> randomInt(10))
                );
                multiGetShardRequest.locations = instance.locations;
                multiGetShardRequest.items = instance.items;
                return multiGetShardRequest;
            }
            case 5 -> {
                var multiGetShardRequest = new MultiGetShardRequest(
                    multiGetRequest,
                    randomValueOtherThan(instance.index(), ESTestCase::randomIdentifier),
                    instance.shardId()
                );
                multiGetShardRequest.locations = instance.locations;
                multiGetShardRequest.items = instance.items;
                return multiGetShardRequest;
            }
            case 6 -> {
                var multiGetShardRequest = new MultiGetShardRequest(multiGetRequest, instance.index(), instance.shardId());
                int numItems = iterations(10, 30);
                var items = randomValueOtherThan(instance.items, () -> createRandomItems(numItems));
                for (int i = 0; i < numItems; i++) {
                    multiGetShardRequest.add(i, items.get(i));
                }
                return multiGetShardRequest;
            }
            default -> throw new IllegalStateException("Unexpected mutation branch value: " + mutationBranch);
        }
    }

    public void testForceSyntheticUnsupported() {
        MultiGetShardRequest request = createTestInstance(true);
        StreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.V_8_3_0);
        Exception e = expectThrows(IllegalArgumentException.class, () -> request.writeTo(out));
        assertEquals(e.getMessage(), "force_synthetic_source is not supported before 8.4.0");
    }

    static MultiGetShardRequest createTestInstance(boolean forceSyntheticSource) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        if (randomBoolean()) {
            multiGetRequest.preference(randomAlphaOfLength(randomIntBetween(1, 10)));
        }
        if (randomBoolean()) {
            multiGetRequest.realtime(false);
        }
        if (randomBoolean()) {
            multiGetRequest.refresh(true);
        }
        if (forceSyntheticSource) {
            multiGetRequest.setForceSyntheticSource(true);
        }
        MultiGetShardRequest multiGetShardRequest = new MultiGetShardRequest(multiGetRequest, "index", 0);
        int numItems = iterations(10, 30);
        var items = createRandomItems(numItems);
        for (int i = 0; i < numItems; i++) {
            multiGetShardRequest.add(i, items.get(i));
        }
        return multiGetShardRequest;
    }

    private static List<MultiGetRequest.Item> createRandomItems(int numItems) {
        List<MultiGetRequest.Item> items = new ArrayList<>(numItems);
        for (int i = 0; i < numItems; i++) {
            MultiGetRequest.Item item = new MultiGetRequest.Item("alias-" + randomAlphaOfLength(randomIntBetween(1, 10)), "id-" + i);
            if (randomBoolean()) {
                int numFields = randomIntBetween(1, 5);
                String[] fields = new String[numFields];
                for (int j = 0; j < fields.length; j++) {
                    fields[j] = randomAlphaOfLength(randomIntBetween(1, 10));
                }
                item.storedFields(fields);
            }
            if (randomBoolean()) {
                item.version(randomIntBetween(1, Integer.MAX_VALUE));
                item.versionType(randomFrom(VersionType.values()));
            }
            if (randomBoolean()) {
                item.fetchSourceContext(FetchSourceContext.of(randomBoolean()));
            }
            items.add(item);
        }
        return items;
    }

    @Override
    protected Writeable.Reader<MultiGetShardRequest> instanceReader() {
        return MultiGetShardRequest::new;
    }
}
