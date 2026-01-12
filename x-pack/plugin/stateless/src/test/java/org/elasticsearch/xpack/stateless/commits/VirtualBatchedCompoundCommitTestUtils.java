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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.commits;

import java.util.List;

public class VirtualBatchedCompoundCommitTestUtils {

    private VirtualBatchedCompoundCommitTestUtils() {}

    public static BlobLocation getBlobLocation(VirtualBatchedCompoundCommit target, String fileName) {
        return target.getBlobLocation(fileName);
    }

    public static List<StatelessCompoundCommit> getPendingStatelessCompoundCommits(VirtualBatchedCompoundCommit target) {
        return target.getPendingCompoundCommits().stream().map(cc -> cc.getStatelessCompoundCommit()).toList();
    }
}
