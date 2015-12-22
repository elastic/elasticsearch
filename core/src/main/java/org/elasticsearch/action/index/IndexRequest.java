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

package org.elasticsearch.action.index;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.TimestampParsingException;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Index request to index a typed JSON document into a specific index and make it searchable. Best
 * created using {@link org.elasticsearch.client.Requests#indexRequest(String)}.
 *
 * The index requires the {@link #index()}, {@link #type(String)}, {@link #id(String)} and
 * {@link #source(byte[])} to be set.
 *
 * The source (content to index) can be set in its bytes form using ({@link #source(byte[])}),
 * its string form ({@link #source(String)}) or using a {@link org.elasticsearch.common.xcontent.XContentBuilder}
 * ({@link #source(org.elasticsearch.common.xcontent.XContentBuilder)}).
 *
 * If the {@link #id(String)} is not set, it will be automatically generated.
 *
 * @see IndexResponse
 * @see org.elasticsearch.client.Requests#indexRequest(String)
 * @see org.elasticsearch.client.Client#index(IndexRequest)
 */
public class IndexRequest extends ReplicationRequest<IndexRequest> implements DocumentRequest<IndexRequest> {

    /**
     * Operation type controls if the type of the index operation.
     */
    public static enum OpType {
        /**
         * Index the source. If there an existing document with the id, it will
         * be replaced.
         */
        INDEX((byte) 0),
        /**
         * Creates the resource. Simply adds it to the index, if there is an existing
         * document with the id, then it won't be removed.
         */
        CREATE((byte) 1);

        private final byte id;
        private final String lowercase;

        OpType(byte id) {
            this.id = id;
            this.lowercase = this.toString().toLowerCase(Locale.ENGLISH);
        }

        /**
         * The internal representation of the operation type.
         */
        public byte id() {
            return id;
        }

        public String lowercase() {
            return this.lowercase;
        }

        /**
         * Constructs the operation type from its internal representation.
         */
        public static OpType fromId(byte id) {
            if (id == 0) {
                return INDEX;
            } else if (id == 1) {
                return CREATE;
            } else {
                throw new IllegalArgumentException("No type match for [" + id + "]");
            }
        }

        public static OpType fromString(String sOpType) {
            String lowersOpType = sOpType.toLowerCase(Locale.ROOT);
            switch (lowersOpType) {
                case "create":
                    return OpType.CREATE;
                case "index":
                    return OpType.INDEX;
                default:
                    throw new IllegalArgumentException("opType [" + sOpType + "] not allowed, either [index] or [create] are allowed");
            }
        }

    }

    private String type;
    private String id;
    @Nullable
    private String routing;
    @Nullable
    private String parent;
    @Nullable
    private String timestamp;
    @Nullable
    private TimeValue ttl;

    private BytesReference source;

    private OpType opType = OpType.INDEX;

    private boolean refresh = false;
    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;

    private XContentType contentType = Requests.INDEX_CONTENT_TYPE;

    public IndexRequest() {
    }

    /**
     * Creates an index request caused by some other request, which is provided as an
     * argument so that its headers and context can be copied to the new request
     */
    public IndexRequest(ActionRequest request) {
        super(request);
    }

    /**
     * Copy constructor that creates a new index request that is a copy of the one provided as an argument.
     * The new request will inherit though headers and context from the original request that caused it.
     */
    public IndexRequest(IndexRequest indexRequest, ActionRequest originalRequest) {
        super(indexRequest, originalRequest);
        this.type = indexRequest.type;
        this.id = indexRequest.id;
        this.routing = indexRequest.routing;
        this.parent = indexRequest.parent;
        this.timestamp = indexRequest.timestamp;
        this.ttl = indexRequest.ttl;
        this.source = indexRequest.source;
        this.opType = indexRequest.opType;
        this.refresh = indexRequest.refresh;
        this.version = indexRequest.version;
        this.versionType = indexRequest.versionType;
        this.contentType = indexRequest.contentType;
    }

    /**
     * Constructs a new index request against the specific index. The {@link #type(String)}
     * {@link #source(byte[])} must be set.
     */
    public IndexRequest(String index) {
        this.index = index;
    }

    /**
     * Constructs a new index request against the specific index and type. The
     * {@link #source(byte[])} must be set.
     */
    public IndexRequest(String index, String type) {
        this.index = index;
        this.type = type;
    }

    /**
     * Constructs a new index request against the index, type, id and using the source.
     *
     * @param index The index to index into
     * @param type  The type to index into
     * @param id    The id of document
     */
    public IndexRequest(String index, String type, String id) {
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
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }

        if (opType() == OpType.CREATE) {
            if (versionType != VersionType.INTERNAL || version != Versions.MATCH_DELETED) {
                validationException = addValidationError("create operations do not support versioning. use index instead", validationException);
                return validationException;
            }
        }

        if (!versionType.validateVersionForWrites(version)) {
            validationException = addValidationError("illegal version value [" + version + "] for version type [" + versionType.name() + "]", validationException);
        }

        if (ttl != null) {
            if (ttl.millis() < 0) {
                validationException = addValidationError("ttl must not be negative", validationException);
            }
        }
        return validationException;
    }

    /**
     * Sets the content type that will be used when generating a document from user provided objects (like Map).
     */
    public IndexRequest contentType(XContentType contentType) {
        this.contentType = contentType;
        return this;
    }

    /**
     * The type of the indexed document.
     */
    @Override
    public String type() {
        return type;
    }

    /**
     * Sets the type of the indexed document.
     */
    public IndexRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * The id of the indexed document. If not set, will be automatically generated.
     */
    @Override
    public String id() {
        return id;
    }

    /**
     * Sets the id of the indexed document. If not set, will be automatically generated.
     */
    public IndexRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    @Override
    public IndexRequest routing(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    @Override
    public String routing() {
        return this.routing;
    }

    /**
     * Sets the parent id of this document.
     */
    public IndexRequest parent(String parent) {
        this.parent = parent;
        return this;
    }

    public String parent() {
        return this.parent;
    }

    /**
     * Sets the timestamp either as millis since the epoch, or, in the configured date format.
     */
    public IndexRequest timestamp(String timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public String timestamp() {
        return this.timestamp;
    }

    /**
     * Sets the ttl value as a time value expression.
     */
    public IndexRequest ttl(String ttl) {
        this.ttl = TimeValue.parseTimeValue(ttl, null, "ttl");
        return this;
    }

    /**
     * Sets the ttl as a {@link TimeValue} instance.
     */
    public IndexRequest ttl(TimeValue ttl) {
        this.ttl = ttl;
        return this;
    }

    /**
     * Sets the relative ttl value in milliseconds. It musts be greater than 0 as it makes little sense otherwise.
     */
    public IndexRequest ttl(long ttl) {
        this.ttl = new TimeValue(ttl);
        return this;
    }

    /**
     * Returns the ttl as a {@link TimeValue}
     */
    public TimeValue ttl() {
        return this.ttl;
    }

    /**
     * The source of the document to index, recopied to a new array if it is unsage.
     */
    public BytesReference source() {
        return source;
    }

    public Map<String, Object> sourceAsMap() {
        return XContentHelper.convertToMap(source, false).v2();
    }

    /**
     * Index the Map as a {@link org.elasticsearch.client.Requests#INDEX_CONTENT_TYPE}.
     *
     * @param source The map to index
     */
    public IndexRequest source(Map source) throws ElasticsearchGenerationException {
        return source(source, contentType);
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     */
    public IndexRequest source(Map source, XContentType contentType) throws ElasticsearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the document source to index.
     *
     * Note, its preferable to either set it using {@link #source(org.elasticsearch.common.xcontent.XContentBuilder)}
     * or using the {@link #source(byte[])}.
     */
    public IndexRequest source(String source) {
        this.source = new BytesArray(source.getBytes(StandardCharsets.UTF_8));
        return this;
    }

    /**
     * Sets the content source to index.
     */
    public IndexRequest source(XContentBuilder sourceBuilder) {
        source = sourceBuilder.bytes();
        return this;
    }

    public IndexRequest source(String field1, Object value1) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.startObject().field(field1, value1).endObject();
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    public IndexRequest source(String field1, Object value1, String field2, Object value2) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.startObject().field(field1, value1).field(field2, value2).endObject();
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    public IndexRequest source(String field1, Object value1, String field2, Object value2, String field3, Object value3) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.startObject().field(field1, value1).field(field2, value2).field(field3, value3).endObject();
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    public IndexRequest source(String field1, Object value1, String field2, Object value2, String field3, Object value3, String field4, Object value4) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.startObject().field(field1, value1).field(field2, value2).field(field3, value3).field(field4, value4).endObject();
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    public IndexRequest source(Object... source) {
        if (source.length % 2 != 0) {
            throw new IllegalArgumentException("The number of object passed must be even but was [" + source.length + "]");
        }
        if (source.length == 2 && source[0] instanceof BytesReference && source[1] instanceof Boolean) {
            throw new IllegalArgumentException("you are using the removed method for source with bytes and unsafe flag, the unsafe flag was removed, please just use source(BytesReference)");
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.startObject();
            for (int i = 0; i < source.length; i++) {
                builder.field(source[i++].toString(), source[i]);
            }
            builder.endObject();
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    /**
     * Sets the document to index in bytes form.
     */
    public IndexRequest source(BytesReference source) {
        this.source = source;
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     */
    public IndexRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    /**
     * Sets the document to index in bytes form (assumed to be safe to be used from different
     * threads).
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     */
    public IndexRequest source(byte[] source, int offset, int length) {
        this.source = new BytesArray(source, offset, length);
        return this;
    }

    /**
     * Sets the type of operation to perform.
     */
    public IndexRequest opType(OpType opType) {
        this.opType = opType;
        if (opType == OpType.CREATE) {
            version(Versions.MATCH_DELETED);
            versionType(VersionType.INTERNAL);
        }
        return this;
    }

    /**
     * Sets a string representation of the {@link #opType(org.elasticsearch.action.index.IndexRequest.OpType)}. Can
     * be either "index" or "create".
     */
    public IndexRequest opType(String opType) {
        return opType(OpType.fromString(opType));
    }


    /**
     * Set to <tt>true</tt> to force this index to use {@link OpType#CREATE}.
     */
    public IndexRequest create(boolean create) {
        if (create) {
            return opType(OpType.CREATE);
        } else {
            return opType(OpType.INDEX);
        }
    }

    /**
     * The type of operation to perform.
     */
    public OpType opType() {
        return this.opType;
    }

    /**
     * Should a refresh be executed post this index operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public IndexRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    /**
     * Sets the version, which will cause the index operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public IndexRequest version(long version) {
        this.version = version;
        return this;
    }

    public long version() {
        return this.version;
    }

    /**
     * Sets the versioning type. Defaults to {@link VersionType#INTERNAL}.
     */
    public IndexRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public VersionType versionType() {
        return this.versionType;
    }

    private Version getVersion(MetaData metaData, String concreteIndex) {
        // this can go away in 3.0 but is here now for easy backporting - since in 2.x we need the version on the timestamp stuff
        final IndexMetaData indexMetaData = metaData.getIndices().get(concreteIndex);
        if (indexMetaData == null) {
            throw new IndexNotFoundException(concreteIndex);
        }
        return Version.indexCreated(indexMetaData.getSettings());
    }

    public void process(MetaData metaData, @Nullable MappingMetaData mappingMd, boolean allowIdGeneration, String concreteIndex) {
        // resolve the routing if needed
        routing(metaData.resolveIndexRouting(parent, routing, index));

        // resolve timestamp if provided externally
        if (timestamp != null) {
            timestamp = MappingMetaData.Timestamp.parseStringTimestamp(timestamp,
                    mappingMd != null ? mappingMd.timestamp().dateTimeFormatter() : TimestampFieldMapper.Defaults.DATE_TIME_FORMATTER,
                    getVersion(metaData, concreteIndex));
        }
        // extract values if needed
        if (mappingMd != null) {
            MappingMetaData.ParseContext parseContext = mappingMd.createParseContext(id, routing, timestamp);

            if (parseContext.shouldParse()) {
                XContentParser parser = null;
                try {
                    parser = XContentHelper.createParser(source);
                    mappingMd.parse(parser, parseContext);
                    if (parseContext.shouldParseId()) {
                        id = parseContext.id();
                    }
                    if (parseContext.shouldParseRouting()) {
                        if (routing != null && !routing.equals(parseContext.routing())) {
                            throw new MapperParsingException("The provided routing value [" + routing + "] doesn't match the routing key stored in the document: [" + parseContext.routing() + "]");
                        }
                        routing = parseContext.routing();
                    }
                    if (parseContext.shouldParseTimestamp()) {
                        timestamp = parseContext.timestamp();
                        if (timestamp != null) {
                            timestamp = MappingMetaData.Timestamp.parseStringTimestamp(timestamp, mappingMd.timestamp().dateTimeFormatter(), getVersion(metaData, concreteIndex));
                        }
                    }
                } catch (MapperParsingException e) {
                    throw e;
                } catch (Exception e) {
                    throw new ElasticsearchParseException("failed to parse doc to extract routing/timestamp/id", e);
                } finally {
                    if (parser != null) {
                        parser.close();
                    }
                }
            }

            // might as well check for routing here
            if (mappingMd.routing().required() && routing == null) {
                throw new RoutingMissingException(concreteIndex, type, id);
            }

            if (parent != null && !mappingMd.hasParentField()) {
                throw new IllegalArgumentException("Can't specify parent if no parent field has been configured");
            }
        } else {
            if (parent != null) {
                throw new IllegalArgumentException("Can't specify parent if no parent field has been configured");
            }
        }

        // generate id if not already provided and id generation is allowed
        if (allowIdGeneration) {
            if (id == null) {
                id(Strings.base64UUID());
            }
        }

        // generate timestamp if not provided, we always have one post this stage...
        if (timestamp == null) {
            String defaultTimestamp = TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP;
            if (mappingMd != null && mappingMd.timestamp() != null) {
                // If we explicitly ask to reject null timestamp
                if (mappingMd.timestamp().ignoreMissing() != null && mappingMd.timestamp().ignoreMissing() == false) {
                    throw new TimestampParsingException("timestamp is required by mapping");
                }
                defaultTimestamp = mappingMd.timestamp().defaultTimestamp();
            }

            if (defaultTimestamp.equals(TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP)) {
                timestamp = Long.toString(System.currentTimeMillis());
            } else {
                timestamp = MappingMetaData.Timestamp.parseStringTimestamp(defaultTimestamp, mappingMd.timestamp().dateTimeFormatter(), getVersion(metaData, concreteIndex));
            }
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        type = in.readString();
        id = in.readOptionalString();
        routing = in.readOptionalString();
        parent = in.readOptionalString();
        timestamp = in.readOptionalString();
        ttl = in.readBoolean() ? TimeValue.readTimeValue(in) : null;
        source = in.readBytesReference();

        opType = OpType.fromId(in.readByte());
        refresh = in.readBoolean();
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(type);
        out.writeOptionalString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(parent);
        out.writeOptionalString(timestamp);
        if (ttl == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            ttl.writeTo(out);
        }
        out.writeBytesReference(source);
        out.writeByte(opType.id());
        out.writeBoolean(refresh);
        out.writeLong(version);
        out.writeByte(versionType.getValue());
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(source, false);
        } catch (Exception e) {
            // ignore
        }
        return "index {[" + index + "][" + type + "][" + id + "], source[" + sSource + "]}";
    }
}
