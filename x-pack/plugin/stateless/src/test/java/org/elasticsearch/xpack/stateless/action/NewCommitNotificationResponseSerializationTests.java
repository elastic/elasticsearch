/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests;

import java.io.IOException;
import java.util.stream.Collectors;

public class NewCommitNotificationResponseSerializationTests extends AbstractWireSerializingTestCase<NewCommitNotificationResponse> {

    @Override
    protected Writeable.Reader<NewCommitNotificationResponse> instanceReader() {
        return NewCommitNotificationResponse::new;
    }

    @Override
    protected NewCommitNotificationResponse createTestInstance() {
        return new NewCommitNotificationResponse(randomSet(0, 10, PrimaryTermAndGenerationTests::randomPrimaryTermAndGeneration));
    }

    @Override
    protected NewCommitNotificationResponse mutateInstance(NewCommitNotificationResponse instance) throws IOException {
        if (instance.getPrimaryTermAndGenerationsInUse().isEmpty()) {
            return new NewCommitNotificationResponse(randomSet(1, 10, PrimaryTermAndGenerationTests::randomPrimaryTermAndGeneration));
        }

        return new NewCommitNotificationResponse(
            instance.getPrimaryTermAndGenerationsInUse()
                .stream()
                .map(PrimaryTermAndGenerationTests::mutatePrimaryTermAndGeneration)
                .collect(Collectors.toSet())
        );
    }
}
