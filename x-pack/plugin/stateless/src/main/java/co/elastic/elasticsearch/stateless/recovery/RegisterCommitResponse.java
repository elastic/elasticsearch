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

import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Objects;

public class RegisterCommitResponse extends ActionResponse {

    public static final RegisterCommitResponse EMPTY = new RegisterCommitResponse(PrimaryTermAndGeneration.ZERO, null);

    /**
     * The primary term/generation of the latest batched compound commit that has been uploaded. If the batch contains multiple compound
     * commits, this is the primary term/generation of the first one.
     */
    private final PrimaryTermAndGeneration latestUploadedBatchedCompoundCommitTermAndGen;

    /**
     * The compound commit to use for the search shard recovery
     */
    private final StatelessCompoundCommit compoundCommit;

    /**
     * Holds the node id to which the register commit request was sent to and on which the recovery commit was registered onto. The node id
     * is not serialized over the wire, and is set back when the search node receives the response.
     */
    private final @Nullable String nodeId;

    private RegisterCommitResponse(PrimaryTermAndGeneration lastUploaded, StatelessCompoundCommit compoundCommit, @Nullable String nodeId) {
        this.latestUploadedBatchedCompoundCommitTermAndGen = Objects.requireNonNull(lastUploaded);
        this.compoundCommit = compoundCommit;
        this.nodeId = nodeId;
    }

    public RegisterCommitResponse(PrimaryTermAndGeneration lastUploaded, StatelessCompoundCommit compoundCommit) {
        this(lastUploaded, compoundCommit, null);
    }

    public RegisterCommitResponse(StreamInput in, @Nullable String nodeId) throws IOException {
        super(in);
        this.latestUploadedBatchedCompoundCommitTermAndGen = new PrimaryTermAndGeneration(in);
        this.compoundCommit = in.readOptionalWriteable(input -> StatelessCompoundCommit.readFromTransport(in));
        this.nodeId = nodeId; // not serialized
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.latestUploadedBatchedCompoundCommitTermAndGen.writeTo(out);
        out.writeOptionalWriteable(this.compoundCommit);
        // node id is not serialized
    }

    public PrimaryTermAndGeneration getLatestUploadedBatchedCompoundCommitTermAndGen() {
        return latestUploadedBatchedCompoundCommitTermAndGen;
    }

    @Nullable
    public StatelessCompoundCommit getCompoundCommit() {
        return compoundCommit;
    }

    /**
     * @return the node id on which the recovery commit was registered onto
     */
    @Nullable
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "RegisterCommitResponse ["
            + "lastUploaded="
            + latestUploadedBatchedCompoundCommitTermAndGen
            + ", compoundCommit="
            + (compoundCommit != null ? compoundCommit.toShortDescription() : "null")
            + ", nodeId="
            + nodeId
            + ']';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegisterCommitResponse that = (RegisterCommitResponse) o;
        return Objects.equals(latestUploadedBatchedCompoundCommitTermAndGen, that.latestUploadedBatchedCompoundCommitTermAndGen)
            && Objects.equals(compoundCommit, that.compoundCommit)
            && Objects.equals(nodeId, that.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latestUploadedBatchedCompoundCommitTermAndGen, compoundCommit, nodeId);
    }
}
