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

package co.elastic.elasticsearch.stateless.action;

import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.unpromotable.BroadcastUnpromotableRequest;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Objects;

import static co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions.NEW_COMMIT_NOTIFICATION_WITH_BCC_INFO;
import static org.elasticsearch.action.ValidateActions.addValidationError;

public class NewCommitNotificationRequest extends BroadcastUnpromotableRequest {
    private final StatelessCompoundCommit compoundCommit;
    // The generation of the BCC that contains the CC. The BCC's primary term is the same as the CC's primary.
    private final long batchedCompoundCommitGeneration;
    // The generation of latest uploaded BCC. It is null if no upload has happened.
    @Nullable
    private final PrimaryTermAndGeneration latestUploadedBatchedCompoundCommitTermAndGen;

    public NewCommitNotificationRequest(
        final IndexShardRoutingTable indexShardRoutingTable,
        final StatelessCompoundCommit compoundCommit,
        final long batchedCompoundCommitGeneration,
        @Nullable final PrimaryTermAndGeneration latestUploadedBatchedCompoundCommitTermAndGen
    ) {
        super(indexShardRoutingTable);
        this.compoundCommit = compoundCommit;
        this.batchedCompoundCommitGeneration = batchedCompoundCommitGeneration;
        this.latestUploadedBatchedCompoundCommitTermAndGen = latestUploadedBatchedCompoundCommitTermAndGen;
    }

    public NewCommitNotificationRequest(final StreamInput in) throws IOException {
        super(in);
        compoundCommit = StatelessCompoundCommit.readFromTransport(in);
        if (in.getTransportVersion().onOrAfter(NEW_COMMIT_NOTIFICATION_WITH_BCC_INFO)) {
            batchedCompoundCommitGeneration = in.readVLong();
            latestUploadedBatchedCompoundCommitTermAndGen = in.readOptionalWriteable(PrimaryTermAndGeneration::new);
        } else {
            batchedCompoundCommitGeneration = compoundCommit.generation();
            latestUploadedBatchedCompoundCommitTermAndGen = compoundCommit.primaryTermAndGeneration();
        }
    }

    public long getTerm() {
        return compoundCommit.primaryTerm();
    }

    public long getGeneration() {
        return compoundCommit.generation();
    }

    public StatelessCompoundCommit getCompoundCommit() {
        return compoundCommit;
    }

    public long getBatchedCompoundCommitGeneration() {
        return batchedCompoundCommitGeneration;
    }

    public PrimaryTermAndGeneration getLatestUploadedBatchedCompoundCommitTermAndGen() {
        return latestUploadedBatchedCompoundCommitTermAndGen;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (getGeneration() < batchedCompoundCommitGeneration) {
            validationException = addValidationError(
                "compound commit generation ["
                    + compoundCommit.generation()
                    + "] < batched compound commit generation ["
                    + batchedCompoundCommitGeneration
                    + "]",
                validationException
            );
        }

        if (latestUploadedBatchedCompoundCommitTermAndGen != null) {
            if (getTerm() < latestUploadedBatchedCompoundCommitTermAndGen.primaryTerm()) {
                validationException = addValidationError(
                    "batched compound commit primary term ["
                        + getTerm()
                        + "] < latest uploaded batched compound commit primary term ["
                        + latestUploadedBatchedCompoundCommitTermAndGen.primaryTerm()
                        + "]",
                    validationException
                );
            } else if (getTerm() == latestUploadedBatchedCompoundCommitTermAndGen.primaryTerm()
                && batchedCompoundCommitGeneration < latestUploadedBatchedCompoundCommitTermAndGen.generation()) {
                    validationException = addValidationError(
                        "batched compound commit generation ["
                            + batchedCompoundCommitGeneration
                            + "] < latest uploaded batched compound commit generation ["
                            + latestUploadedBatchedCompoundCommitTermAndGen.generation()
                            + "]",
                        validationException
                    );
                }
        }

        return validationException;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        compoundCommit.writeTo(out);
        if (out.getTransportVersion().onOrAfter(NEW_COMMIT_NOTIFICATION_WITH_BCC_INFO)) {
            out.writeVLong(batchedCompoundCommitGeneration);
            out.writeOptionalWriteable(latestUploadedBatchedCompoundCommitTermAndGen);
        } else {
            // The CC must be packaged as a singleton BCC
            assert compoundCommit.generation() == batchedCompoundCommitGeneration
                : compoundCommit.generation() + "!=" + batchedCompoundCommitGeneration;
            assert compoundCommit.primaryTermAndGeneration().equals(latestUploadedBatchedCompoundCommitTermAndGen)
                : compoundCommit.primaryTermAndGeneration() + "!=" + latestUploadedBatchedCompoundCommitTermAndGen;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NewCommitNotificationRequest request = (NewCommitNotificationRequest) o;
        return batchedCompoundCommitGeneration == request.batchedCompoundCommitGeneration
            && Objects.equals(compoundCommit, request.compoundCommit)
            && Objects.equals(latestUploadedBatchedCompoundCommitTermAndGen, request.latestUploadedBatchedCompoundCommitTermAndGen);
    }

    @Override
    public int hashCode() {
        return Objects.hash(compoundCommit, batchedCompoundCommitGeneration, latestUploadedBatchedCompoundCommitTermAndGen);
    }

    @Override
    public String toString() {
        return "NewCommitNotificationRequest{"
            + "compoundCommit="
            + compoundCommit
            + ", batchedCompoundCommitGeneration="
            + batchedCompoundCommitGeneration
            + ", latestUploadedBatchedCompoundCommitTermAndGen="
            + latestUploadedBatchedCompoundCommitTermAndGen
            + '}';
    }
}
