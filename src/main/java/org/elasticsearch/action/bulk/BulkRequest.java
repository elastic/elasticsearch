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

package org.elasticsearch.action.bulk;

import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A bulk request holds an ordered {@link IndexRequest}s and {@link DeleteRequest}s and allows to executes
 * it in a single batch.
 *
 * @see org.elasticsearch.client.Client#bulk(BulkRequest)
 */
public class BulkRequest implements ActionRequest {

    final List<ActionRequest> requests = Lists.newArrayList();

    private boolean listenerThreaded = false;

    private ReplicationType replicationType = ReplicationType.DEFAULT;
    private WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;
    private boolean refresh = false;

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkRequest add(IndexRequest request) {
        request.beforeLocalFork();
        return internalAdd(request);
    }

    private BulkRequest internalAdd(IndexRequest request) {
        requests.add(request);
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkRequest add(DeleteRequest request) {
        requests.add(request);
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(byte[] data, int from, int length, boolean contentUnsafe) throws Exception {
        return add(data, from, length, contentUnsafe, null, null);
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(byte[] data, int from, int length, boolean contentUnsafe, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        XContent xContent = XContentFactory.xContent(data, from, length);
        byte marker = xContent.streamSeparator();
        while (true) {
            int nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }
            // now parse the action
            XContentParser parser = xContent.createParser(data, from, nextMarker - from);

            try {
                // move pointers
                from = nextMarker + 1;

                // Move to START_OBJECT
                XContentParser.Token token = parser.nextToken();
                if (token == null) {
                    continue;
                }
                assert token == XContentParser.Token.START_OBJECT;
                // Move to FIELD_NAME, that's the action
                token = parser.nextToken();
                assert token == XContentParser.Token.FIELD_NAME;
                String action = parser.currentName();

                String index = defaultIndex;
                String type = defaultType;
                String id = null;
                String routing = null;
                String parent = null;
                String timestamp = null;
                Long ttl = null;
                String opType = null;
                long version = 0;
                VersionType versionType = VersionType.INTERNAL;
                String percolate = null;

                // at this stage, next token can either be END_OBJECT (and use default index and type, with auto generated id)
                // or START_OBJECT which will have another set of parameters

                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("_index".equals(currentFieldName)) {
                            index = parser.text();
                        } else if ("_type".equals(currentFieldName)) {
                            type = parser.text();
                        } else if ("_id".equals(currentFieldName)) {
                            id = parser.text();
                        } else if ("_routing".equals(currentFieldName) || "routing".equals(currentFieldName)) {
                            routing = parser.text();
                        } else if ("_parent".equals(currentFieldName) || "parent".equals(currentFieldName)) {
                            parent = parser.text();
                        } else if ("_timestamp".equals(currentFieldName) || "timestamp".equals(currentFieldName)) {
                            timestamp = parser.text();
                        } else if ("_ttl".equals(currentFieldName) || "ttl".equals(currentFieldName)) {
                            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                                ttl = TimeValue.parseTimeValue(parser.text(), null).millis();
                            } else {
                                ttl = parser.longValue();
                            }
                        } else if ("op_type".equals(currentFieldName) || "opType".equals(currentFieldName)) {
                            opType = parser.text();
                        } else if ("_version".equals(currentFieldName) || "version".equals(currentFieldName)) {
                            version = parser.longValue();
                        } else if ("_version_type".equals(currentFieldName) || "_versionType".equals(currentFieldName) || "version_type".equals(currentFieldName) || "versionType".equals(currentFieldName)) {
                            versionType = VersionType.fromString(parser.text());
                        } else if ("percolate".equals(currentFieldName) || "_percolate".equals(currentFieldName)) {
                            percolate = parser.textOrNull();
                        }
                    }
                }

                if ("delete".equals(action)) {
                    add(new DeleteRequest(index, type, id).parent(parent).version(version).versionType(versionType).routing(routing));
                } else {
                    nextMarker = findNextMarker(marker, from, data, length);
                    if (nextMarker == -1) {
                        break;
                    }
                    // order is important, we set parent after routing, so routing will be set to parent if not set explicitly
                    // we use internalAdd so we don't fork here, this allows us not to copy over the big byte array to small chunks
                    // of index request. All index requests are still unsafe if applicable.
                    if ("index".equals(action)) {
                        if (opType == null) {
                            internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).timestamp(timestamp).ttl(ttl).version(version).versionType(versionType)
                                    .source(data, from, nextMarker - from, contentUnsafe)
                                    .percolate(percolate));
                        } else {
                            internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).timestamp(timestamp).ttl(ttl).version(version).versionType(versionType)
                                    .create("create".equals(opType))
                                    .source(data, from, nextMarker - from, contentUnsafe)
                                    .percolate(percolate));
                        }
                    } else if ("create".equals(action)) {
                        internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).timestamp(timestamp).ttl(ttl).version(version).versionType(versionType)
                                .create(true)
                                .source(data, from, nextMarker - from, contentUnsafe)
                                .percolate(percolate));
                    }
                    // move pointers
                    from = nextMarker + 1;
                }
            } finally {
                parser.close();
            }
        }
        return this;
    }

    /**
     * Sets the consistency level of write. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    public BulkRequest consistencyLevel(WriteConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    public WriteConsistencyLevel consistencyLevel() {
        return this.consistencyLevel;
    }

    /**
     * Should a refresh be executed post this bulk operation causing the operations to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public BulkRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    /**
     * Set the replication type for this operation.
     */
    public BulkRequest replicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return this;
    }

    public ReplicationType replicationType() {
        return this.replicationType;
    }

    private int findNextMarker(byte marker, int from, byte[] data, int length) {
        for (int i = from; i < length; i++) {
            if (data[i] == marker) {
                return i;
            }
        }
        return -1;
    }

    public int numberOfActions() {
        return requests.size();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (int i = 0; i < requests.size(); i++) {
            ActionRequestValidationException ex = requests.get(i).validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
        }

        return validationException;
    }

    @Override
    public boolean listenerThreaded() {
        return listenerThreaded;
    }

    @Override
    public BulkRequest listenerThreaded(boolean listenerThreaded) {
        this.listenerThreaded = listenerThreaded;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        replicationType = ReplicationType.fromId(in.readByte());
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            byte type = in.readByte();
            if (type == 0) {
                IndexRequest request = new IndexRequest();
                request.readFrom(in);
                requests.add(request);
            } else if (type == 1) {
                DeleteRequest request = new DeleteRequest();
                request.readFrom(in);
                requests.add(request);
            }
        }
        refresh = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(replicationType.id());
        out.writeByte(consistencyLevel.id());
        out.writeVInt(requests.size());
        for (ActionRequest request : requests) {
            if (request instanceof IndexRequest) {
                out.writeByte((byte) 0);
            } else if (request instanceof DeleteRequest) {
                out.writeByte((byte) 1);
            }
            request.writeTo(out);
        }
        out.writeBoolean(refresh);
    }
}
