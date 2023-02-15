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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.unpromotable.BroadcastUnpromotableRequest;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class NewCommitNotificationRequest extends BroadcastUnpromotableRequest {
    private final long term;
    private final long generation;
    private final Map<String, StoreFileMetadata> commitFiles;

    public NewCommitNotificationRequest(
        final IndexShardRoutingTable indexShardRoutingTable,
        final long term,
        final long generation,
        final Map<String, StoreFileMetadata> commitFiles
    ) {
        super(indexShardRoutingTable);
        this.term = term;
        this.generation = generation;
        this.commitFiles = commitFiles;
    }

    public NewCommitNotificationRequest(final StreamInput in) throws IOException {
        super(in);
        term = in.readVLong();
        generation = in.readVLong();
        commitFiles = in.readImmutableMap(StreamInput::readString, StoreFileMetadata::new);
    }

    public long getTerm() {
        return term;
    }

    public long getGeneration() {
        return generation;
    }

    public Map<String, StoreFileMetadata> getFiles() {
        return commitFiles;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (term < 0) {
            validationException = addValidationError("term is negative", validationException);
        }
        if (generation < 0) {
            validationException = addValidationError("generation is negative", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(term);
        out.writeVLong(generation);
        out.writeMap(commitFiles, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public String toString() {
        return "NotifyRequest{"
            + "shardId="
            + shardId()
            + ", term="
            + term
            + ", generation="
            + generation
            + ", commitFiles="
            + commitFiles
            + '}';
    }
}
