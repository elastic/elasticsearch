/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dataset;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.dataset.PutDatasetAction.Request;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Round-trip wire test for {@link Request} ({@code AcknowledgedRequest} subclass, transported
 * to the master).
 */
public class PutDatasetActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            randomName(),
            randomName(),
            "s3://" + randomAlphaOfLength(6) + "/" + randomAlphaOfLength(4) + "/*.parquet",
            randomBoolean() ? null : randomAlphaOfLengthBetween(0, 20),
            randomSettings()
        );
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return switch (between(0, 4)) {
            case 0 -> new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.name() + "_mutated",
                instance.dataSource(),
                instance.resource(),
                instance.description(),
                instance.rawSettings()
            );
            case 1 -> new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.name(),
                instance.dataSource() + "_mutated",
                instance.resource(),
                instance.description(),
                instance.rawSettings()
            );
            case 2 -> new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.name(),
                instance.dataSource(),
                instance.resource() + "/extra",
                instance.description(),
                instance.rawSettings()
            );
            case 3 -> new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.name(),
                instance.dataSource(),
                instance.resource(),
                randomValueOtherThan(instance.description(), () -> randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20)),
                instance.rawSettings()
            );
            case 4 -> {
                Map<String, Object> mutated = new HashMap<>(instance.rawSettings());
                mutated.put(randomAlphaOfLength(6), randomAlphaOfLength(6));
                yield new Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    instance.name(),
                    instance.dataSource(),
                    instance.resource(),
                    instance.description(),
                    mutated
                );
            }
            default -> throw new AssertionError("unreachable");
        };
    }

    private static String randomName() {
        return randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT);
    }

    private static Map<String, Object> randomSettings() {
        Map<String, Object> out = new HashMap<>();
        int count = between(0, 5);
        for (int i = 0; i < count; i++) {
            out.put(randomAlphaOfLength(6), randomAlphaOfLength(8));
        }
        return out;
    }
}
