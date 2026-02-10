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

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.blobNameFromGeneration;
import static org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests.mutatePrimaryTermAndGeneration;
import static org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGenerationTests.randomPrimaryTermAndGeneration;

public class BlobFileTests extends AbstractWireSerializingTestCase<BlobFile> {
    @Override
    protected Writeable.Reader<BlobFile> instanceReader() {
        return BlobFile::new;
    }

    @Override
    protected BlobFile createTestInstance() {
        return randomBlobFile();
    }

    private BlobFile randomBlobFile() {
        var termAndGen = randomPrimaryTermAndGeneration();
        return new BlobFile(blobNameFromGeneration(termAndGen.generation()), termAndGen);
    }

    @Override
    protected BlobFile mutateInstance(BlobFile instance) throws IOException {
        var mutatedTermAndGen = mutatePrimaryTermAndGeneration(instance.termAndGeneration());
        return new BlobFile(blobNameFromGeneration(mutatedTermAndGen.generation()), mutatedTermAndGen);
    }

    public void testValidation() {
        expectThrows(AssertionError.class, () -> new BlobFile(randomAlphaOfLengthBetween(0, 30), randomPrimaryTermAndGeneration()));
    }
}
