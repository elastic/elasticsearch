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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;

/**
 * Represents a file (typically a {@link BatchedCompoundCommit}) stored in the blobstore.
 */
public record BlobFile(String blobName, PrimaryTermAndGeneration termAndGeneration) implements Writeable {

    public BlobFile {
        assert (StatelessCompoundCommit.startsWithBlobPrefix(blobName) == false && termAndGeneration.generation() == -1)
            || termAndGeneration.generation() == StatelessCompoundCommit.parseGenerationFromBlobName(blobName)
            : "generation mismatch: " + termAndGeneration + " vs " + blobName;
    }

    public BlobFile(StreamInput in) throws IOException {
        this(in.readString(), new PrimaryTermAndGeneration(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(blobName);
        termAndGeneration.writeTo(out);
    }

    public long primaryTerm() {
        return termAndGeneration.primaryTerm();
    }

    /**
     * The generation of the blob file in case it is a stateless compound commit file, otherwise it is -1.
     */
    public long generation() {
        return termAndGeneration.generation();
    }

    @Override
    public String toString() {
        return "BlobFile{" + "primaryTerm=" + primaryTerm() + ", blobName='" + blobName + '\'' + '}';
    }

}
