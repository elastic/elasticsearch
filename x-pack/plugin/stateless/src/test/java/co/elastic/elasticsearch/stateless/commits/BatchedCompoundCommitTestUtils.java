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

package co.elastic.elasticsearch.stateless.commits;

import java.io.Closeable;

public final class BatchedCompoundCommitTestUtils {

    private BatchedCompoundCommitTestUtils() {}

    public static Closeable disablePaddingAsserter() {
        var previous = BatchedCompoundCommit.PADDING_ASSERTER;
        BatchedCompoundCommit.PADDING_ASSERTER = (blobName, blobLength, blobReader, offset, compoundCommit) -> true;
        return () -> BatchedCompoundCommit.PADDING_ASSERTER = previous;
    }
}
