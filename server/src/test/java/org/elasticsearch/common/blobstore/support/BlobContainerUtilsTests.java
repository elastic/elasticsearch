/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.support;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.blobstore.support.BlobContainerUtils.MAX_REGISTER_CONTENT_LENGTH;
import static org.elasticsearch.common.blobstore.support.BlobContainerUtils.ensureValidRegisterContent;
import static org.elasticsearch.common.blobstore.support.BlobContainerUtils.getRegisterUsingConsistentRead;

public class BlobContainerUtilsTests extends ESTestCase {

    public void testEnsureValidRegisterContent() {
        ensureValidRegisterContent(randomBytesReference(between(0, MAX_REGISTER_CONTENT_LENGTH)));

        final var invalidContent = randomBytesReference(MAX_REGISTER_CONTENT_LENGTH + 1);
        expectThrows(Throwable.class, () -> ensureValidRegisterContent(invalidContent));
    }

    public void testGetRegisterUsingConsistentRead() throws IOException {
        final var content = randomBytesReference(between(0, MAX_REGISTER_CONTENT_LENGTH));
        assertEquals(content, getRegisterUsingConsistentRead(content.streamInput(), "", ""));

        final var invalidContent = randomBytesReference(MAX_REGISTER_CONTENT_LENGTH + 1);
        expectThrows(IllegalStateException.class, () -> getRegisterUsingConsistentRead(invalidContent.streamInput(), "", ""));
    }

}
