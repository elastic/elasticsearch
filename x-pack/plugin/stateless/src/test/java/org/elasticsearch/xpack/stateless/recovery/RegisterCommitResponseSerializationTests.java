/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommitTestUtils;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGenerationTests;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGenerationTests.randomPrimaryTermAndGeneration;

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
