/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

public class BlobLocationTestUtils {

    private BlobLocationTestUtils() {}

    public static BlobLocation createBlobLocation(long primaryTerm, long generation, long offset, long fileLength) {
        return new BlobLocation(
            new BlobFile(StatelessCompoundCommit.PREFIX + generation, new PrimaryTermAndGeneration(primaryTerm, generation)),
            offset,
            fileLength
        );
    }

    public static BlobFileRanges createBlobFileRanges(long primaryTerm, long generation, long offset, long fileLength) {
        return new BlobFileRanges(createBlobLocation(primaryTerm, generation, offset, fileLength));
    }
}
