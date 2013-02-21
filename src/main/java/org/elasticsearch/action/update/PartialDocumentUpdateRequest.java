/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.update;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 */
public class PartialDocumentUpdateRequest extends InstanceShardOperationRequest<PartialDocumentUpdateRequest> {

    private String type;
    private String id;
    @Nullable
    private String routing;

    private String[] fields;

    int retryOnConflict = 0;

    private String percolate;

    private boolean refresh = false;

    private IndexRequest upsertRequest;

    @Nullable
    private IndexRequest doc;
    
    private ReplicationType replicationType = ReplicationType.DEFAULT;
    private WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;

    public PartialDocumentUpdateRequest() {

    }

    public PartialDocumentUpdateRequest(String index, String type, String id) {
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
        if (doc == null) {
            validationException = addValidationError("doc is missing", validationException);
        }
        return validationException;
    }

    /**
     * The type of the indexed document.
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the type of the indexed document.
     */
    public PartialDocumentUpdateRequest setType(String type) {
        this.type = type;
        return this;
    }

    /**
     * The id of the indexed document.
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the id of the indexed document.
     */
    public PartialDocumentUpdateRequest setId(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public PartialDocumentUpdateRequest setRouting(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    /**
     * Sets the parent id of this document. Will simply set the routing to this value, as it is only
     * used for routing with delete requests.
     */
    public PartialDocumentUpdateRequest setParent(String parent) {
        if (routing == null) {
            routing = parent;
        }
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public String getRouting() {
        return this.routing;
    }

    public int getShardId() {
        return this.shardId;
    }


    /**
     * Explicitly specify the fields that will be returned. By default, nothing is returned.
     */
    public PartialDocumentUpdateRequest setFields(String... fields) {
        this.fields = fields;
        return this;
    }

    /**
     * Get the fields to be returned.
     */
    public String[] getFields() {
        return this.fields;
    }

    /**
     * Sets the number of retries of a version conflict occurs because the document was updated between
     * getting it and updating it. Defaults to 1.
     */
    public PartialDocumentUpdateRequest setRetryOnConflict(int retryOnConflict) {
        this.retryOnConflict = retryOnConflict;
        return this;
    }

    public int getRetryOnConflict() {
        return this.retryOnConflict;
    }

    /**
     * Causes the update request document to be percolated. The parameter is the percolate query
     * to use to reduce the percolated queries that are going to run against this doc. Can be
     * set to <tt>*</tt> to indicate that all percolate queries should be run.
     */
    public PartialDocumentUpdateRequest setPercolate(String percolate) {
        this.percolate = percolate;
        return this;
    }

    public String getPercolate() {
        return this.percolate;
    }

    /**
     * Should a refresh be executed post this update operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public PartialDocumentUpdateRequest setRefresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean isRefresh() {
        return this.refresh;
    }
    
    /**
     * The replication type.
     */
    public ReplicationType setReplicationType() {
        return this.replicationType;
    }

    /**
     * Sets the replication type.
     */
    public PartialDocumentUpdateRequest setReplicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return this;
    }

    public WriteConsistencyLevel getConsistencyLevel() {
        return this.consistencyLevel;
    }

    /**
     * Sets the consistency level of write. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    public PartialDocumentUpdateRequest setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public PartialDocumentUpdateRequest setDoc(IndexRequest doc) {
        this.doc = doc;
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public PartialDocumentUpdateRequest setDoc(XContentBuilder source) {
        getSafeDoc().setSource(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public PartialDocumentUpdateRequest setDoc(Map source) {
        getSafeDoc().setSource(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public PartialDocumentUpdateRequest setDoc(Map source, XContentType contentType) {
        getSafeDoc().setSource(source, contentType);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public PartialDocumentUpdateRequest setDoc(String source) {
        getSafeDoc().setSource(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public PartialDocumentUpdateRequest setDoc(byte[] source) {
        getSafeDoc().setSource(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public PartialDocumentUpdateRequest setDoc(byte[] source, int offset, int length) {
        getSafeDoc().setSource(source, offset, length);
        return this;
    }

    public IndexRequest getDoc() {
        return this.doc;
    }

    private IndexRequest getSafeDoc() {
        if (doc == null) {
            doc = new IndexRequest();
        }
        return doc;
    }

    /**
     * Sets the index request to be used if the document does not exists. Otherwise, a {@link org.elasticsearch.index.engine.DocumentMissingException}
     * is thrown.
     */
    public PartialDocumentUpdateRequest setUpsertRequest(IndexRequest upsertRequest) {
        this.upsertRequest = upsertRequest;
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public PartialDocumentUpdateRequest setUpsertRequest(XContentBuilder source) {
        getSafeUpsertRequest().setSource(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public PartialDocumentUpdateRequest setUpsertRequest(Map source) {
        getSafeUpsertRequest().setSource(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public PartialDocumentUpdateRequest setUpsertRequest(Map source, XContentType contentType) {
        getSafeUpsertRequest().setSource(source, contentType);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public PartialDocumentUpdateRequest setUpsertRequest(String source) {
        getSafeUpsertRequest().setSource(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public PartialDocumentUpdateRequest setUpsertRequest(byte[] source) {
        getSafeUpsertRequest().setSource(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public PartialDocumentUpdateRequest setUpsertRequest(byte[] source, int offset, int length) {
        getSafeUpsertRequest().setSource(source, offset, length);
        return this;
    }

    public IndexRequest getUpsertRequest() {
        return this.upsertRequest;
    }

    private IndexRequest getSafeUpsertRequest() {
        if (upsertRequest == null) {
            upsertRequest = new IndexRequest();
        }
        return upsertRequest;
    }

    public PartialDocumentUpdateRequest setSource(XContentBuilder source) throws Exception {
        return setSource(source.bytes());
    }

    public PartialDocumentUpdateRequest setSource(byte[] source) throws Exception {
        return setSource(source, 0, source.length);
    }

    public PartialDocumentUpdateRequest setSource(byte[] source, int offset, int length) throws Exception {
        return setSource(new BytesArray(source, offset, length));
    }

    public PartialDocumentUpdateRequest setSource(BytesReference source) throws Exception {
        XContentType xContentType = XContentFactory.xContentType(source);
        XContentParser parser = XContentFactory.xContent(xContentType).createParser(source);
        try {
            XContentParser.Token t = parser.nextToken();
            if (t == null) {
                return this;
            }
            String currentFieldName = null;
            while ((t = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (t == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("upsert".equals(currentFieldName)) {
                    XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
                    builder.copyCurrentStructure(parser);
                    getSafeUpsertRequest().setSource(builder);
                } else if ("doc".equals(currentFieldName)) {
                    XContentBuilder docBuilder = XContentFactory.contentBuilder(xContentType);
                    docBuilder.copyCurrentStructure(parser);
                    getSafeDoc().setSource(docBuilder);
                }
            }
        } finally {
            parser.close();
        }
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        replicationType = ReplicationType.fromId(in.readByte());
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        type = in.readString();
        id = in.readString();
        routing = in.readOptionalString();
        retryOnConflict = in.readVInt();
        percolate = in.readOptionalString();
        refresh = in.readBoolean();
        if (in.readBoolean()) {
            doc = new IndexRequest();
            doc.readFrom(in);
        }
        int size = in.readInt();
        if (size >= 0) {
            fields = new String[size];
            for (int i = 0; i < size; i++) {
                fields[i] = in.readString();
            }
        }
        if (in.readBoolean()) {
            upsertRequest = new IndexRequest();
            upsertRequest.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(replicationType.id());
        out.writeByte(consistencyLevel.id());
        out.writeString(type);
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeVInt(retryOnConflict);
        out.writeOptionalString(percolate);
        out.writeBoolean(refresh);
        if (doc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            // make sure the basics are set
            doc.setIndex(index);
            doc.setType(type);
            doc.setId(id);
            doc.writeTo(out);
        }
        if (fields == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(fields.length);
            for (String field : fields) {
                out.writeString(field);
            }
        }
        if (upsertRequest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            // make sure the basics are set
            upsertRequest.setIndex(index);
            upsertRequest.setType(type);
            upsertRequest.setId(id);
            upsertRequest.writeTo(out);
        }
    }
}
