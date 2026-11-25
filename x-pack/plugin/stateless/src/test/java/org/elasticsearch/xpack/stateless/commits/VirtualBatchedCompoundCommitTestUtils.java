/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import java.io.InputStream;
import java.util.List;

public class VirtualBatchedCompoundCommitTestUtils {

    private VirtualBatchedCompoundCommitTestUtils() {}

    public static BlobLocation getBlobLocation(VirtualBatchedCompoundCommit target, String fileName) {
        return target.getBlobLocation(fileName);
    }

    public static List<StatelessCompoundCommit> getPendingStatelessCompoundCommits(VirtualBatchedCompoundCommit target) {
        return target.getPendingCompoundCommits().stream().map(cc -> cc.getStatelessCompoundCommit()).toList();
    }

    public static long getHeaderSize(VirtualBatchedCompoundCommit.PendingCompoundCommit pendingCompoundCommit) {
        return pendingCompoundCommit.getHeaderSize();
    }

    public static InputStream getInputStreamForUpload(VirtualBatchedCompoundCommit vbcc) {
        return vbcc.getInputStreamForUpload();
    }
}
