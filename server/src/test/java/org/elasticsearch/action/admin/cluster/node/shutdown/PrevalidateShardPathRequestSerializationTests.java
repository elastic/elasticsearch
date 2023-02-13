/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.Supplier;

public class PrevalidateShardPathRequestSerializationTests extends AbstractWireSerializingTestCase<PrevalidateShardPathRequest> {

    @Override
    protected Writeable.Reader<PrevalidateShardPathRequest> instanceReader() {
        return PrevalidateShardPathRequest::new;
    }

    @Override
    protected PrevalidateShardPathRequest createTestInstance() {
        Set<ShardId> shardIds = randomSet(0, 100, PrevalidateShardPathRequestSerializationTests::randomShardId);
        String[] nodeIds = randomArray(1, 5, String[]::new, () -> randomAlphaOfLength(20));
        PrevalidateShardPathRequest request = new PrevalidateShardPathRequest(shardIds, nodeIds);
        return randomBoolean() ? request : request.timeout(randomTimeValue());
    }

    @Override
    protected PrevalidateShardPathRequest mutateInstance(PrevalidateShardPathRequest request) {
        int i = randomInt(2);
        return switch (i) {
            case 0 -> new PrevalidateShardPathRequest(
                createSetMutation(request.getShardIds(), PrevalidateShardPathRequestSerializationTests::randomShardId),
                request.nodesIds()
            ).timeout(request.timeout());
            case 1 -> new PrevalidateShardPathRequest(
                request.getShardIds(),
                createArrayMutation(request.nodesIds(), () -> randomAlphaOfLength(20), String[]::new)
            ).timeout(request.timeout());
            case 2 -> new PrevalidateShardPathRequest(request.getShardIds(), request.nodesIds()).timeout(
                randomValueOtherThan(request.timeout(), () -> new TimeValue(randomLongBetween(1000, 10000)))
            );
            default -> throw new IllegalStateException("unexpected value: " + i);
        };
    }

    public static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(20), UUIDs.randomBase64UUID(), randomIntBetween(0, 25));
    }

    public static <T> void mutateList(List<T> list, Supplier<T> supplier) {
        if (list.size() > 0 && randomBoolean()) {
            // just remove one
            list.remove(randomInt(list.size() - 1));
        } else {
            list.add(supplier.get());
        }
    }

    public static <T> Set<T> createSetMutation(Set<T> set, Supplier<T> supplier) {
        List<T> list = new ArrayList<>(set);
        mutateList(list, supplier);
        return new HashSet<>(list);
    }

    public static <T> T[] createArrayMutation(T[] array, Supplier<T> supplier, IntFunction<T[]> arrayConstructor) {
        List<T> list = new ArrayList<>(Arrays.asList(array));
        mutateList(list, supplier);
        return list.toArray(arrayConstructor.apply(list.size()));
    }
}
