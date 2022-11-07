/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class CheckShardsOnDataPathResponseSerializationTests extends AbstractWireSerializingTestCase<CheckShardsOnDataPathResponse> {

    @Override
    protected Writeable.Reader<CheckShardsOnDataPathResponse> instanceReader() {
        return CheckShardsOnDataPathResponse::new;
    }

    @Override
    protected CheckShardsOnDataPathResponse createTestInstance() {
        return new CheckShardsOnDataPathResponse(
            new ClusterName(randomAlphaOfLength(25)),
            randomList(0, 5, NodeCheckShardsOnDataPathResponseSerializationTests::getRandomResponse),
            List.of()// randomList(0, 5, CheckShardsOnDataPathResponseSerializationTests::getRandomFailedNodeException)
        );
    }

    @Override
    protected CheckShardsOnDataPathResponse mutateInstance(CheckShardsOnDataPathResponse response) throws IOException {
        int i = randomInt(2);
        return switch (i) {
            case 0 -> new CheckShardsOnDataPathResponse(new ClusterName(randomAlphaOfLength(20)), response.getNodes(), response.failures());
            case 1 -> new CheckShardsOnDataPathResponse(
                response.getClusterName(),
                createListMutation(response.getNodes(), NodeCheckShardsOnDataPathResponseSerializationTests::getRandomResponse),
                response.failures()
            );
            case 2 -> new CheckShardsOnDataPathResponse(
                response.getClusterName(),
                response.getNodes(),
                createListMutation(response.failures(), CheckShardsOnDataPathResponseSerializationTests::getRandomFailedNodeException)
            );
            default -> throw new IllegalStateException("unexpected value: " + i);
        };
    }

    public static FailedNodeException getRandomFailedNodeException() {
        return new FailedNodeException(randomAlphaOfLength(10), randomAlphaOfLengthBetween(0, 1000), randomException());
    }

    public static Throwable randomException() {
        return randomFrom(
            new ConnectException(randomAlphaOfLengthBetween(10, 100)),
            new IOException("file not found"),
            new OutOfMemoryError("out of space"),
            new IllegalArgumentException("index must be between 10 and 100")
        );
    }

    public static <T> List<T> createListMutation(List<T> list, Supplier<T> supplier) {
        List<T> copy = new ArrayList<>(list);
        if (copy.size() > 0 && randomBoolean()) {
            // just remove one
            copy.remove(randomInt(copy.size() - 1));
        } else {
            copy.add(supplier.get());
        }
        return copy;
    }
}
