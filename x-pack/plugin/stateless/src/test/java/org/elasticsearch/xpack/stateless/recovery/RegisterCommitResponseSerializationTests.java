/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests.randomPrimaryTermAndGeneration;

public class RegisterCommitResponseSerializationTests extends AbstractWireSerializingTestCase<RegisterCommitResponse> {

    @Override
    protected Writeable.Reader<RegisterCommitResponse> instanceReader() {
        return in -> new RegisterCommitResponse(in, null);
    }

    @Override
    protected RegisterCommitResponse createTestInstance() {
        return new RegisterCommitResponse(
            randomPrimaryTermAndGeneration(),
            randomBoolean() ? StatelessCompoundCommitTestUtils.randomCompoundCommit() : null
        );
    }

    @Override
    protected RegisterCommitResponse mutateInstance(RegisterCommitResponse instance) throws IOException {
        int i = randomIntBetween(0, 1);
        return switch (i) {
            case 0 -> new RegisterCommitResponse(
                randomValueOtherThan(
                    instance.getLatestUploadedBatchedCompoundCommitTermAndGen(),
                    PrimaryTermAndGenerationTests::randomPrimaryTermAndGeneration
                ),
                instance.getCompoundCommit()
            );
            case 1 -> new RegisterCommitResponse(
                instance.getLatestUploadedBatchedCompoundCommitTermAndGen(),
                randomValueOtherThan(
                    instance.getCompoundCommit(),
                    () -> randomBoolean() ? StatelessCompoundCommitTestUtils.randomCompoundCommit() : null
                )
            );
            default -> throw new IllegalStateException("Unexpected value " + i);
        };
    }
}
