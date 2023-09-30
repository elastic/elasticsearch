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

import org.elasticsearch.action.support.broadcast.unpromotable.BroadcastUnpromotableRequest;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NewCommitNotificationRequest extends BroadcastUnpromotableRequest {
    private final StatelessCompoundCommit compoundCommit;

    public NewCommitNotificationRequest(final IndexShardRoutingTable indexShardRoutingTable, final StatelessCompoundCommit compoundCommit) {
        super(indexShardRoutingTable);
        this.compoundCommit = compoundCommit;
    }

    public NewCommitNotificationRequest(final StreamInput in) throws IOException {
        super(in);
        compoundCommit = StatelessCompoundCommit.readFromTransport(in);
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

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        compoundCommit.writeTo(out);
    }

    @Override
    public String toString() {
        return "NewCommitNotificationRequest" + compoundCommit;
    }
}
