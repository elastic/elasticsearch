/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.delete;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to delete a document from an index based on its type and id. Best created using
 * {@link org.elasticsearch.client.Requests#deleteRequest(String)}.
 * <p>
 * The operation requires the {@link #index()}, {@link #type(String)} and {@link #id(String)} to
 * be set.
 *
 * @see DeleteResponse
 * @see org.elasticsearch.client.Client#delete(DeleteRequest)
 * @see org.elasticsearch.client.Requests#deleteRequest(String)
 */
public class DeleteRequest extends ReplicatedWriteRequest<DeleteRequest> implements DocWriteRequest<DeleteRequest>, CompositeIndicesRequest {

    private String type;
    private String id;
    @Nullable
    private String routing;
    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;
    private long casSeqNp = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long casPrimaryTerm = 0;

    public DeleteRequest() {
    }

    /**
     * Constructs a new delete request against the specified index. The {@link #type(String)} and {@link #id(String)}
     * must be set.
     */
    public DeleteRequest(String index) {
        this.index = index;
    }

    /**
     * Constructs a new delete request against the specified index with the type and id.
     *
     * @param index The index to get the document from
     * @param type  The type of the document
     * @param id    The id of the document
     */
    public DeleteRequest(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (id == null) {
            validationException = addValidationError("id is missing", validationException);
        }
        if (!versionType.validateVersionForWrites(version)) {
            validationException = addValidationError("illegal version value [" + version + "] for version type [" + versionType.name() + "]", validationException);
        }
        if (versionType == VersionType.FORCE) {
            validationException = addValidationError("version type [force] may no longer be used", validationException);
        }

        if (casSeqNp != SequenceNumbers.UNASSIGNED_SEQ_NO && (
            versionType != VersionType.INTERNAL || version != Versions.MATCH_ANY
        )) {
            validationException = addValidationError("compare and write operations can not use versioning", validationException);
        }
        return validationException;
    }

    /**
     * The type of the document to delete.
     */
    @Override
    public String type() {
        return type;
    }

    /**
     * Sets the type of the document to delete.
     */
    public DeleteRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * The id of the document to delete.
     */
    @Override
    public String id() {
        return id;
    }

    /**
     * Sets the id of the document to delete.
     */
    public DeleteRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    @Override
    public DeleteRequest routing(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    /**
     * Controls the shard routing of the delete request. Using this value to hash the shard
     * and not the id.
     */
    @Override
    public String routing() {
        return this.routing;
    }

    @Override
    public DeleteRequest version(long version) {
        this.version = version;
        return this;
    }

    @Override
    public long version() {
        return this.version;
    }

    @Override
    public DeleteRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public long casSeqNo() {
        return casSeqNp;
    }

    public long casPrimaryTerm() {
        return casPrimaryTerm;
    }

    public DeleteRequest compareAndSet(long seqNo, long term) {
        if (term == 0 && seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            throw new IllegalArgumentException("seqNo is set, but primary term is [0]");
        }
        if (term != 0 && seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            throw new IllegalArgumentException("seqNo is unassigned, but primary term is [" + term + "]");
        }
        if (seqNo < 0 && seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            throw new IllegalArgumentException("sequence numbers must be non negative. got [" +  seqNo + "].");
        }
        if (term < 0) {
            throw new IllegalArgumentException("primary term must be non negative. got [" + term + "]");
        }
        casSeqNp = seqNo;
        casPrimaryTerm = term;
        return this;
    }

    @Override
    public VersionType versionType() {
        return this.versionType;
    }

    @Override
    public OpType opType() {
        return OpType.DELETE;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        type = in.readString();
        id = in.readString();
        routing = in.readOptionalString();
        if (in.getVersion().before(Version.V_7_0_0_alpha1)) {
            in.readOptionalString(); // _parent
        }
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
        if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            casSeqNp = in.readZLong();
            casPrimaryTerm = in.readVLong();
        } else {
            casSeqNp = SequenceNumbers.UNASSIGNED_SEQ_NO;
            casPrimaryTerm = 0;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(type);
        out.writeString(id);
        out.writeOptionalString(routing());
        if (out.getVersion().before(Version.V_7_0_0_alpha1)) {
            out.writeOptionalString(null); // _parent
        }
        out.writeLong(version);
        out.writeByte(versionType.getValue());
        if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            out.writeZLong(casSeqNp);
            out.writeVLong(casPrimaryTerm);
        } else if (casSeqNp != SequenceNumbers.UNASSIGNED_SEQ_NO || casPrimaryTerm != 0) {
            assert false : "compareAndSet [" + casSeqNp + "], currentDocTem [" + casPrimaryTerm + "]";
            throw new IllegalStateException(
                "sequence number based compare and write is not supported until all nodes are on version 7.0 or higher. " +
                    "Stream version [" + out.getVersion() + "]");
        }
    }

    @Override
    public String toString() {
        return "delete {[" + index + "][" + type + "][" + id + "]}";
    }

    /**
     * Override this method from ReplicationAction, this is where we are storing our state in the request object (which we really shouldn't
     * do). Once the transport client goes away we can move away from making this available, but in the meantime this is dangerous to set or
     * use because the DeleteRequest object will always be wrapped in a bulk request envelope, which is where this *should* be set.
     */
    @Override
    public DeleteRequest setShardId(ShardId shardId) {
        throw new UnsupportedOperationException("shard id should never be set on DeleteRequest");
    }
}
