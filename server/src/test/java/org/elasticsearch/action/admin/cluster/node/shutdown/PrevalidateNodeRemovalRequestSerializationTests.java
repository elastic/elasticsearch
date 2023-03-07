/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Arrays;

public class PrevalidateNodeRemovalRequestSerializationTests extends AbstractWireSerializingTestCase<PrevalidateNodeRemovalRequest> {

    @Override
    protected Writeable.Reader<PrevalidateNodeRemovalRequest> instanceReader() {
        return PrevalidateNodeRemovalRequest::new;
    }

    @Override
    protected PrevalidateNodeRemovalRequest createTestInstance() {
        return randomRequest();
    }

    @Override
    protected PrevalidateNodeRemovalRequest mutateInstance(PrevalidateNodeRemovalRequest request) {
        int i = randomIntBetween(0, 2);
        return switch (i) {
            case 0 -> PrevalidateNodeRemovalRequest.builder()
                .setNames(
                    randomValueOtherThanMany(
                        input -> Arrays.equals(input, request.getNames()),
                        PrevalidateNodeRemovalRequestSerializationTests::randomStringArray
                    )
                )
                .setIds(request.getIds())
                .setExternalIds(request.getExternalIds())
                .build();

            case 1 -> PrevalidateNodeRemovalRequest.builder()
                .setNames(request.getNames())
                .setIds(
                    randomValueOtherThanMany(
                        input -> Arrays.equals(input, request.getIds()),
                        PrevalidateNodeRemovalRequestSerializationTests::randomStringArray
                    )
                )
                .setExternalIds(request.getExternalIds())
                .build();

            case 2 -> PrevalidateNodeRemovalRequest.builder()
                .setNames(request.getNames())
                .setIds(request.getIds())
                .setExternalIds(
                    randomValueOtherThanMany(
                        input -> Arrays.equals(input, request.getExternalIds()),
                        PrevalidateNodeRemovalRequestSerializationTests::randomStringArray
                    )
                )
                .build();
            default -> throw new IllegalStateException("unexpected value: " + i);
        };
    }

    private static String[] randomStringArray() {
        return randomArray(0, 20, String[]::new, () -> randomAlphaOfLengthBetween(5, 20));
    }

    private static PrevalidateNodeRemovalRequest randomRequest() {
        return PrevalidateNodeRemovalRequest.builder()
            .setNames(randomStringArray())
            .setIds(randomStringArray())
            .setExternalIds(randomStringArray())
            .build();
    }
}
