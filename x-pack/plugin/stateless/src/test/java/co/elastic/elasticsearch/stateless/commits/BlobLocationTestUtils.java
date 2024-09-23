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

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

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
