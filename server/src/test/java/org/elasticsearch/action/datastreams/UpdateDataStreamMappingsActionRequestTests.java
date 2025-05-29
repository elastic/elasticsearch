/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.cluster.metadata.ComponentTemplateTests;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.ComponentTemplateTests.randomMappings;

public class UpdateDataStreamMappingsActionRequestTests extends AbstractWireSerializingTestCase<UpdateDataStreamMappingsAction.Request> {
    @Override
    protected Writeable.Reader<UpdateDataStreamMappingsAction.Request> instanceReader() {
        return UpdateDataStreamMappingsAction.Request::new;
    }

    @Override
    protected UpdateDataStreamMappingsAction.Request createTestInstance() {
        UpdateDataStreamMappingsAction.Request request = new UpdateDataStreamMappingsAction.Request(
            randomMappings(),
            randomBoolean(),
            randomTimeValue(),
            randomTimeValue()
        );
        request.indices(randomIndices());
        return request;
    }

    @Override
    protected UpdateDataStreamMappingsAction.Request mutateInstance(UpdateDataStreamMappingsAction.Request instance) throws IOException {
        String[] indices = instance.indices();
        CompressedXContent mappings = instance.getMappings();
        boolean dryRun = instance.isDryRun();
        TimeValue masterNodeTimeout = instance.masterNodeTimeout();
        TimeValue ackTimeout = instance.ackTimeout();
        switch (between(0, 4)) {
            case 0 -> {
                indices = randomArrayValueOtherThan(indices, this::randomIndices);
            }
            case 1 -> {
                mappings = randomValueOtherThan(mappings, ComponentTemplateTests::randomMappings);
            }
            case 2 -> {
                dryRun = dryRun == false;
            }
            case 3 -> {
                masterNodeTimeout = randomValueOtherThan(masterNodeTimeout, ESTestCase::randomTimeValue);
            }
            case 4 -> {
                ackTimeout = randomValueOtherThan(ackTimeout, ESTestCase::randomTimeValue);
            }
            default -> throw new AssertionError("Should not be here");
        }
        return new UpdateDataStreamMappingsAction.Request(mappings, dryRun, masterNodeTimeout, ackTimeout).indices(indices);
    }

    private String[] randomIndices() {
        return randomList(10, () -> randomAlphaOfLength(20)).toArray(new String[0]);
    }

    public static <T> T[] randomArrayValueOtherThan(T[] input, Supplier<T[]> randomSupplier) {
        return randomValueOtherThanMany(v -> Arrays.equals(input, v), randomSupplier);
    }
}
