/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import java.util.Map;
import java.util.Set;

public class BlobFileRangesTestUtils {

    public static Map<String, BlobFileRanges> computeBlobFileRanges(
        boolean useReplicatedRanges,
        StatelessCompoundCommit compoundCommit,
        long blobOffset,
        Set<String> internalFiles
    ) {
        return BlobFileRanges.computeBlobFileRanges(useReplicatedRanges, compoundCommit, blobOffset, internalFiles);
    }
}
