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

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class RegisterCommitResponse extends ActionResponse {

    // The primary term and generation of the stateless compound commit that the search shard should use for recovery.
    private final PrimaryTermAndGeneration commit;

    public RegisterCommitResponse(PrimaryTermAndGeneration commit) {
        this.commit = Objects.requireNonNull(commit);
    }

    public RegisterCommitResponse(StreamInput in) throws IOException {
        super(in);
        commit = new PrimaryTermAndGeneration(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commit.writeTo(out);
    }

    public PrimaryTermAndGeneration getCommit() {
        return commit;
    }

    @Override
    public String toString() {
        return "RegisterCommitForRecoveryResponse{" + "commit=" + commit + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(commit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof RegisterCommitResponse == false) return false;
        RegisterCommitResponse other = (RegisterCommitResponse) o;
        return Objects.equals(commit, other.commit);
    }
}
