/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * The realized form of an {@code update} whose partial document touches only {@code doc_values.updatable} fields. Instead of a
 * read-modify-reindex, it applies the field updates to the document's doc-values columns in place. This request is never issued by a
 * user directly: {@code TransportShardBulkAction} produces it on the primary from an {@code UpdateRequest} and it takes the place of
 * that update in the shard-bulk item so replicas replay the same field updates.
 */
public class DocValuesUpdateRequest extends ReplicatedWriteRequest<DocValuesUpdateRequest>
    implements
        DocWriteRequest<DocValuesUpdateRequest> {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(DocValuesUpdateRequest.class);

    private static final ShardId NO_SHARD_ID = null;

    private String id;
    @Nullable
    private String routing;
    private boolean routingFromSlice;
    private long ifSeqNo = UNASSIGNED_SEQ_NO;
    private long ifPrimaryTerm = UNASSIGNED_PRIMARY_TERM;
    // The document's version when the update was prepared; unchanged by the update, carried so response and replica report the same value.
    private long documentVersion = Versions.MATCH_ANY;
    // The operation's own seq_no and primary term (fresh, for replication), assigned on the primary. The document's own seq_no is
    // unchanged.
    private long operationSeqNo = UNASSIGNED_SEQ_NO;
    private long operationPrimaryTerm = UNASSIGNED_PRIMARY_TERM;
    private List<Translog.DocValuesUpdate.FieldUpdate> updates;

    public DocValuesUpdateRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
        id = in.readString();
        routing = in.readOptionalString();
        ifSeqNo = in.readZLong();
        ifPrimaryTerm = in.readVLong();
        documentVersion = in.readZLong();
        operationSeqNo = in.readZLong();
        operationPrimaryTerm = in.readVLong();
        updates = in.readCollectionAsList(Translog.DocValuesUpdate.FieldUpdate::readFrom);
    }

    public DocValuesUpdateRequest(String index, String id, List<Translog.DocValuesUpdate.FieldUpdate> updates) {
        super(NO_SHARD_ID);
        this.index = index;
        this.id = id;
        this.updates = updates;
    }

    public List<Translog.DocValuesUpdate.FieldUpdate> updates() {
        return updates;
    }

    public DocValuesUpdateRequest documentVersion(long documentVersion) {
        this.documentVersion = documentVersion;
        return this;
    }

    public long documentVersion() {
        return documentVersion;
    }

    /**
     * Records the operation's sequence number and primary term, generated on the primary. The replica replays the update at these values.
     */
    public DocValuesUpdateRequest operationSeqNo(long operationSeqNo, long operationPrimaryTerm) {
        this.operationSeqNo = operationSeqNo;
        this.operationPrimaryTerm = operationPrimaryTerm;
        return this;
    }

    public long operationSeqNo() {
        return operationSeqNo;
    }

    public long operationPrimaryTerm() {
        return operationPrimaryTerm;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (Strings.isEmpty(id)) {
            validationException = addValidationError("id is missing", validationException);
        }
        if (updates == null || updates.isEmpty()) {
            validationException = addValidationError("no doc values updates provided", validationException);
        }
        validationException = DocWriteRequest.validateSeqNoBasedCASParams(this, validationException);
        return validationException;
    }

    @Override
    public String id() {
        return id;
    }

    public DocValuesUpdateRequest id(String id) {
        this.id = id;
        return this;
    }

    @Override
    public DocValuesUpdateRequest routing(String routing) {
        this.routing = Strings.isEmpty(routing) ? null : routing;
        return this;
    }

    @Override
    public String routing() {
        return routing;
    }

    @Override
    public DocValuesUpdateRequest setRoutingFromSlice(boolean routingFromSlice) {
        this.routingFromSlice = routingFromSlice;
        return this;
    }

    @Override
    public boolean isRoutingFromSlice() {
        return routingFromSlice;
    }

    // Doc-values updates do not carry an external version; the CAS is expressed via ifSeqNo/ifPrimaryTerm.
    @Override
    public long version() {
        return Versions.MATCH_ANY;
    }

    @Override
    public DocValuesUpdateRequest version(long version) {
        throw new UnsupportedOperationException("doc values updates do not support versioning");
    }

    @Override
    public VersionType versionType() {
        return VersionType.INTERNAL;
    }

    @Override
    public DocValuesUpdateRequest versionType(VersionType versionType) {
        throw new UnsupportedOperationException("doc values updates do not support versioning");
    }

    @Override
    public long ifSeqNo() {
        return ifSeqNo;
    }

    @Override
    public long ifPrimaryTerm() {
        return ifPrimaryTerm;
    }

    @Override
    public DocValuesUpdateRequest setIfSeqNo(long seqNo) {
        if (seqNo < 0 && seqNo != UNASSIGNED_SEQ_NO) {
            throw new IllegalArgumentException("sequence numbers must be non negative. got [" + seqNo + "].");
        }
        ifSeqNo = seqNo;
        return this;
    }

    @Override
    public DocValuesUpdateRequest setIfPrimaryTerm(long term) {
        if (term < 0) {
            throw new IllegalArgumentException("primary term must be non negative. got [" + term + "]");
        }
        ifPrimaryTerm = term;
        return this;
    }

    // An already-realized write: the shard bulk action and its replica dispatch it by concrete type, and it must NOT report UPDATE, or
    // the bulk item processor would try to run the update translation over it again. The user still sees an update response, which is
    // built from the original UpdateRequest (bulk) or by TransportUpdateAction (single-document update).
    @Override
    public OpType opType() {
        return OpType.INDEX;
    }

    @Override
    public boolean isRequireAlias() {
        return false;
    }

    @Override
    public boolean isRequireDataStream() {
        return false;
    }

    @Override
    public int route(IndexRouting indexRouting) {
        return indexRouting.updateShard(id, routing);
    }

    @Override
    public int rerouteAtSourceDuringResharding(IndexRouting indexRouting) {
        return indexRouting.updateShard(id, routing);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeBody(out);
    }

    @Override
    public void writeThin(StreamOutput out) throws IOException {
        super.writeThin(out);
        writeBody(out);
    }

    private void writeBody(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeZLong(ifSeqNo);
        out.writeVLong(ifPrimaryTerm);
        out.writeZLong(documentVersion);
        out.writeZLong(operationSeqNo);
        out.writeVLong(operationPrimaryTerm);
        out.writeCollection(updates);
    }

    @Override
    public String toString() {
        return "doc_values_update {[" + index + "][" + id + "], updates=" + updates + "}";
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE + RamUsageEstimator.sizeOf(id);
        for (Translog.DocValuesUpdate.FieldUpdate update : updates) {
            size += update.estimatedSizeInBytes();
        }
        return size;
    }
}
