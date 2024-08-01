/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.index;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.StringLiteralDeduplicator;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.plugins.internal.XContentParserDecorator;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Index request to index a typed JSON document into a specific index and make it searchable.
 * <p>
 * The index requires the {@link #index()}, {@link #id(String)} and
 * {@link #source(byte[], XContentType)} to be set.
 * <p>
 * The source (content to index) can be set in its bytes form using ({@link #source(byte[], XContentType)}),
 * its string form ({@link #source(String, XContentType)}) or using a {@link org.elasticsearch.xcontent.XContentBuilder}
 * ({@link #source(org.elasticsearch.xcontent.XContentBuilder)}).
 * <p>
 * If the {@link #id(String)} is not set, it will be automatically generated.
 *
 * @see IndexResponse
 * @see org.elasticsearch.client.internal.Client#index(IndexRequest)
 */
public class IndexRequest extends ReplicatedWriteRequest<IndexRequest> implements DocWriteRequest<IndexRequest>, CompositeIndicesRequest {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(IndexRequest.class);
    private static final TransportVersion PIPELINES_HAVE_RUN_FIELD_ADDED = TransportVersions.V_8_10_X;

    /**
     * Max length of the source document to include into string()
     *
     * @see ReplicationRequest#createTask
     */
    static final int MAX_SOURCE_LENGTH_IN_TOSTRING = 2048;

    /**
     * Maximal allowed length (in bytes) of the document ID.
     */
    public static final int MAX_DOCUMENT_ID_LENGTH_IN_BYTES = 512;

    private static final ShardId NO_SHARD_ID = null;

    private String id;
    @Nullable
    private String routing;

    private BytesReference source;

    private OpType opType = OpType.INDEX;

    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;

    private XContentType contentType;

    private String pipeline;
    private String finalPipeline;

    private boolean isPipelineResolved;

    private boolean requireAlias;

    private boolean requireDataStream;

    /**
     * Transient flag denoting that the local request should be routed to a failure store. Not persisted across the wire.
     */
    private boolean writeToFailureStore = false;

    /**
     * This indicates whether the response to this request ought to list the ingest pipelines that were executed on the document
     */
    private boolean listExecutedPipelines;
    /**
     * This holds the names of the ingest pipelines that have been executed on the document for this request. This is not meant to be set by
     * the creator of the request -- pipelines are added here at runtime as they are executed.
     */
    @Nullable
    private List<String> executedPipelines = null;

    /**
     * Value for {@link #getAutoGeneratedTimestamp()} if the document has an external
     * provided ID.
     */
    public static final long UNSET_AUTO_GENERATED_TIMESTAMP = -1L;

    private long autoGeneratedTimestamp = UNSET_AUTO_GENERATED_TIMESTAMP;

    private boolean isRetry = false;
    private long ifSeqNo = UNASSIGNED_SEQ_NO;
    private long ifPrimaryTerm = UNASSIGNED_PRIMARY_TERM;

    private Map<String, String> dynamicTemplates = Map.of();

    /**
     * rawTimestamp field is used on the coordinate node, it doesn't need to be serialised.
     */
    private Object rawTimestamp;
    private long normalisedBytesParsed = -1;
    private boolean originatesFromUpdateByScript;
    private boolean originatesFromUpdateByDoc;

    public IndexRequest(StreamInput in) throws IOException {
        this(null, in);
    }

    public IndexRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            String type = in.readOptionalString();
            assert MapperService.SINGLE_MAPPING_NAME.equals(type) : "Expected [_doc] but received [" + type + "]";
        }
        id = in.readOptionalString();
        routing = in.readOptionalString();
        source = in.readBytesReference();
        opType = OpType.fromId(in.readByte());
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
        pipeline = readPipelineName(in);
        finalPipeline = readPipelineName(in);
        isPipelineResolved = in.readBoolean();
        isRetry = in.readBoolean();
        autoGeneratedTimestamp = in.readLong();
        if (in.readBoolean()) {
            // faster than StreamInput::readEnum, do not replace we read a lot of these instances at times
            contentType = XContentType.ofOrdinal(in.readByte());
        } else {
            contentType = null;
        }
        ifSeqNo = in.readZLong();
        ifPrimaryTerm = in.readVLong();
        requireAlias = in.readBoolean();
        dynamicTemplates = in.readMap(StreamInput::readString);
        if (in.getTransportVersion().onOrAfter(PIPELINES_HAVE_RUN_FIELD_ADDED)
            && in.getTransportVersion().before(TransportVersions.V_8_13_0)) {
            in.readBoolean();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            this.listExecutedPipelines = in.readBoolean();
            if (listExecutedPipelines) {
                List<String> possiblyImmutableExecutedPipelines = in.readOptionalCollectionAsList(StreamInput::readString);
                this.executedPipelines = possiblyImmutableExecutedPipelines == null
                    ? null
                    : new ArrayList<>(possiblyImmutableExecutedPipelines);
            }
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            requireDataStream = in.readBoolean();
            normalisedBytesParsed = in.readZLong();
        } else {
            requireDataStream = false;
        }

        if (in.getTransportVersion().onOrAfter(TransportVersions.INDEX_REQUEST_UPDATE_BY_SCRIPT_ORIGIN)) {
            originatesFromUpdateByScript = in.readBoolean();
        } else {
            originatesFromUpdateByScript = false;
        }

        if (in.getTransportVersion().onOrAfter(TransportVersions.INDEX_REQUEST_UPDATE_BY_DOC_ORIGIN)) {
            originatesFromUpdateByDoc = in.readBoolean();
        } else {
            originatesFromUpdateByDoc = false;
        }
    }

    public IndexRequest() {
        super(NO_SHARD_ID);
    }

    /**
     * Constructs a new index request against the specific index. The
     * {@link #source(byte[], XContentType)} must be set.
     */
    public IndexRequest(String index) {
        super(NO_SHARD_ID);
        this.index = index;
    }

    private static final StringLiteralDeduplicator pipelineNameDeduplicator = new StringLiteralDeduplicator();

    // reads pipeline name from the stream and deduplicates it to save heap on large bulk requests
    @Nullable
    private static String readPipelineName(StreamInput in) throws IOException {
        final String read = in.readOptionalString();
        if (read == null) {
            return null;
        }
        if (IngestService.NOOP_PIPELINE_NAME.equals(read)) {
            // common path of no pipeline set
            return IngestService.NOOP_PIPELINE_NAME;
        }
        return pipelineNameDeduplicator.deduplicate(read);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }
        if (contentType == null) {
            validationException = addValidationError("content type is missing", validationException);
        }
        assert opType == OpType.INDEX || opType == OpType.CREATE : "unexpected op-type: " + opType;
        final long resolvedVersion = resolveVersionDefaults();
        if (opType == OpType.CREATE) {
            if (versionType != VersionType.INTERNAL) {
                validationException = addValidationError(
                    "create operations only support internal versioning. use index instead",
                    validationException
                );
                return validationException;
            }

            if (resolvedVersion != Versions.MATCH_DELETED) {
                validationException = addValidationError(
                    "create operations do not support explicit versions. use index instead",
                    validationException
                );
                return validationException;
            }

            if (ifSeqNo != UNASSIGNED_SEQ_NO || ifPrimaryTerm != UNASSIGNED_PRIMARY_TERM) {
                validationException = addValidationError(
                    "create operations do not support compare and set. use index instead",
                    validationException
                );
                return validationException;
            }
        }

        if (id == null) {
            if (versionType != VersionType.INTERNAL
                || (resolvedVersion != Versions.MATCH_DELETED && resolvedVersion != Versions.MATCH_ANY)) {
                validationException = addValidationError("an id must be provided if version type or value are set", validationException);
            }
        }

        validationException = DocWriteRequest.validateSeqNoBasedCASParams(this, validationException);

        validationException = DocWriteRequest.validateDocIdLength(id, validationException);

        if (pipeline != null && pipeline.isEmpty()) {
            validationException = addValidationError("pipeline cannot be an empty string", validationException);
        }

        if (finalPipeline != null && finalPipeline.isEmpty()) {
            validationException = addValidationError("final pipeline cannot be an empty string", validationException);
        }

        return validationException;
    }

    /**
     * The content type. This will be used when generating a document from user provided objects like Maps and when parsing the
     * source at index time
     */
    public XContentType getContentType() {
        return contentType;
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
        if (routing != null && routing.isEmpty()) {
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
     * Sets the ingest pipeline to be executed before indexing the document
     */
    public IndexRequest setPipeline(String pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    /**
     * Returns the ingest pipeline to be executed before indexing the document
     */
    public String getPipeline() {
        return this.pipeline;
    }

    /**
     * Sets the final ingest pipeline to be executed before indexing the document.
     *
     * @param finalPipeline the name of the final pipeline
     * @return this index request
     */
    public IndexRequest setFinalPipeline(final String finalPipeline) {
        this.finalPipeline = finalPipeline;
        return this;
    }

    /**
     * Returns the final ingest pipeline to be executed before indexing the document.
     *
     * @return the name of the final pipeline
     */
    public String getFinalPipeline() {
        return this.finalPipeline;
    }

    /**
     * Sets if the pipeline for this request has been resolved by the coordinating node.
     *
     * @param isPipelineResolved true if the pipeline has been resolved
     * @return the request
     */
    public IndexRequest isPipelineResolved(final boolean isPipelineResolved) {
        this.isPipelineResolved = isPipelineResolved;
        return this;
    }

    /**
     * Returns whether or not the pipeline for this request has been resolved by the coordinating node.
     *
     * @return true if the pipeline has been resolved
     */
    public boolean isPipelineResolved() {
        return this.isPipelineResolved;
    }

    /**
     * The source of the document to index, recopied to a new array if it is unsafe.
     */
    public BytesReference source() {
        return source;
    }

    public Map<String, Object> sourceAsMap() {
        return XContentHelper.convertToMap(source, false, contentType).v2();
    }

    public Map<String, Object> sourceAsMap(XContentParserDecorator parserDecorator) {
        return XContentHelper.convertToMap(source, false, contentType, parserDecorator).v2();
    }

    /**
     * Index the Map in {@link Requests#INDEX_CONTENT_TYPE} format
     *
     * @param source The map to index
     */
    public IndexRequest source(Map<String, ?> source) throws ElasticsearchGenerationException {
        return source(source, Requests.INDEX_CONTENT_TYPE);
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     */
    public IndexRequest source(Map<String, ?> source, XContentType contentType) throws ElasticsearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public IndexRequest source(Map<String, ?> source, XContentType contentType, boolean ensureNoSelfReferences)
        throws ElasticsearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source, ensureNoSelfReferences);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the document source to index.
     * <p>
     * Note, its preferable to either set it using {@link #source(org.elasticsearch.xcontent.XContentBuilder)}
     * or using the {@link #source(byte[], XContentType)}.
     */
    public IndexRequest source(String source, XContentType xContentType) {
        return source(new BytesArray(source), xContentType);
    }

    /**
     * Sets the content source to index.
     */
    public IndexRequest source(XContentBuilder sourceBuilder) {
        return source(BytesReference.bytes(sourceBuilder), sourceBuilder.contentType());
    }

    /**
     * Sets the content source to index using the default content type ({@link Requests#INDEX_CONTENT_TYPE})
     * <p>
     * <b>Note: the number of objects passed to this method must be an even
     * number. Also the first argument in each pair (the field name) must have a
     * valid String representation.</b>
     * </p>
     */
    public IndexRequest source(Object... source) {
        return source(Requests.INDEX_CONTENT_TYPE, source);
    }

    /**
     * Sets the content source to index.
     * <p>
     * <b>Note: the number of objects passed to this method as varargs must be an even
     * number. Also the first argument in each pair (the field name) must have a
     * valid String representation.</b>
     * </p>
     */
    public IndexRequest source(XContentType xContentType, Object... source) {
        return source(getXContentBuilder(xContentType, source));
    }

    /**
     * Returns an XContentBuilder for the given xContentType and source array
     * <p>
     * <b>Note: the number of objects passed to this method as varargs must be an even
     * number. Also the first argument in each pair (the field name) must have a
     * valid String representation.</b>
     * </p>
     */
    public static XContentBuilder getXContentBuilder(XContentType xContentType, Object... source) {
        if (source.length % 2 != 0) {
            throw new IllegalArgumentException("The number of object passed must be even but was [" + source.length + "]");
        }
        if (source.length == 2 && source[0] instanceof BytesReference && source[1] instanceof Boolean) {
            throw new IllegalArgumentException(
                "you are using the removed method for source with bytes and unsafe flag, the unsafe flag"
                    + " was removed, please just use source(BytesReference)"
            );
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
            builder.startObject();
            // This for loop increments by 2 because the source array contains adjacent key/value pairs:
            for (int i = 0; i < source.length; i = i + 2) {
                String field = source[i].toString();
                Object value = source[i + 1];
                builder.field(field, value);
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    /**
     * Sets the document to index in bytes form.
     */
    public IndexRequest source(BytesReference source, XContentType xContentType) {
        this.source = Objects.requireNonNull(source);
        this.contentType = Objects.requireNonNull(xContentType);
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     */
    public IndexRequest source(byte[] source, XContentType xContentType) {
        return source(source, 0, source.length, xContentType);
    }

    /**
     * Sets the document to index in bytes form (assumed to be safe to be used from different
     * threads).
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     */
    public IndexRequest source(byte[] source, int offset, int length, XContentType xContentType) {
        return source(new BytesArray(source, offset, length), xContentType);
    }

    /**
     * Sets the type of operation to perform.
     */
    public IndexRequest opType(OpType opType) {
        if (opType != OpType.CREATE && opType != OpType.INDEX) {
            throw new IllegalArgumentException("opType must be 'create' or 'index', found: [" + opType + "]");
        }
        this.opType = opType;
        return this;
    }

    /**
     * Sets a string representation of the {@link #opType(OpType)}. Can
     * be either "index" or "create".
     */
    public IndexRequest opType(String opType) {
        String op = opType.toLowerCase(Locale.ROOT);
        if (op.equals("create")) {
            opType(OpType.CREATE);
        } else if (op.equals("index")) {
            opType(OpType.INDEX);
        } else {
            throw new IllegalArgumentException("opType must be 'create' or 'index', found: [" + opType + "]");
        }
        return this;
    }

    /**
     * Set to {@code true} to force this index to use {@link OpType#CREATE}.
     */
    public IndexRequest create(boolean create) {
        if (create) {
            return opType(OpType.CREATE);
        } else {
            return opType(OpType.INDEX);
        }
    }

    @Override
    public OpType opType() {
        return this.opType;
    }

    @Override
    public IndexRequest version(long version) {
        this.version = version;
        return this;
    }

    /**
     * Returns stored version. If currently stored version is {@link Versions#MATCH_ANY} and
     * opType is {@link OpType#CREATE}, returns {@link Versions#MATCH_DELETED}.
     */
    @Override
    public long version() {
        return resolveVersionDefaults();
    }

    /**
     * Resolves the version based on operation type {@link #opType()}.
     */
    private long resolveVersionDefaults() {
        if (opType == OpType.CREATE && version == Versions.MATCH_ANY) {
            return Versions.MATCH_DELETED;
        } else {
            return version;
        }
    }

    @Override
    public IndexRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    /**
     * only perform this indexing request if the document was last modification was assigned the given
     * sequence number. Must be used in combination with {@link #setIfPrimaryTerm(long)}
     * <p>
     * If the document last modification was assigned a different sequence number a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public IndexRequest setIfSeqNo(long seqNo) {
        if (seqNo < 0 && seqNo != UNASSIGNED_SEQ_NO) {
            throw new IllegalArgumentException("sequence numbers must be non negative. got [" + seqNo + "].");
        }
        ifSeqNo = seqNo;
        return this;
    }

    /**
     * only performs this indexing request if the document was last modification was assigned the given
     * primary term. Must be used in combination with {@link #setIfSeqNo(long)}
     * <p>
     * If the document last modification was assigned a different term a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public IndexRequest setIfPrimaryTerm(long term) {
        if (term < 0) {
            throw new IllegalArgumentException("primary term must be non negative. got [" + term + "]");
        }
        ifPrimaryTerm = term;
        return this;
    }

    /**
     * If set, only perform this indexing request if the document was last modification was assigned this sequence number.
     * If the document last modification was assigned a different sequence number a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public long ifSeqNo() {
        return ifSeqNo;
    }

    /**
     * If set, only perform this indexing request if the document was last modification was assigned this primary term.
     * <p>
     * If the document last modification was assigned a different term a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public long ifPrimaryTerm() {
        return ifPrimaryTerm;
    }

    @Override
    public VersionType versionType() {
        return this.versionType;
    }

    @Override
    public void process(IndexRouting indexRouting) {
        indexRouting.process(this);
    }

    /**
     * Set the {@code #id()} to an automatically generated one and make this
     * request compatible with the append-only optimization.
     */
    public void autoGenerateId() {
        assert id == null;
        assert autoGeneratedTimestamp == UNSET_AUTO_GENERATED_TIMESTAMP : "timestamp has already been generated!";
        assert ifSeqNo == UNASSIGNED_SEQ_NO;
        assert ifPrimaryTerm == UNASSIGNED_PRIMARY_TERM;
        /*
         * Set the auto generated timestamp so the append only optimization
         * can quickly test if this request *must* be unique without reaching
         * into the Lucene index. We lock it >0 because UNSET_AUTO_GENERATED_TIMESTAMP
         * has a special meaning and is a negative value. This optimiation will
         * never work before 1970, but that's ok. It's after 1970.
         */
        autoGeneratedTimestamp = Math.max(0, System.currentTimeMillis());
        String uid = UUIDs.base64UUID();
        id(uid);
    }

    /**
     * Resets this <code>IndexRequest</code> class, so that in case this instance can be used by the bulk/index action
     * if it was already used before. For example if retrying a retryable failure.
     */
    public void reset() {
        autoGeneratedTimestamp = UNSET_AUTO_GENERATED_TIMESTAMP;
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
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            out.writeOptionalString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeOptionalString(id);
        out.writeOptionalString(routing);
        out.writeBytesReference(source);
        out.writeByte(opType.getId());
        out.writeLong(version);
        out.writeByte(versionType.getValue());
        out.writeOptionalString(pipeline);
        out.writeOptionalString(finalPipeline);
        out.writeBoolean(isPipelineResolved);
        out.writeBoolean(isRetry);
        out.writeLong(autoGeneratedTimestamp);
        if (contentType != null) {
            out.writeBoolean(true);
            XContentHelper.writeTo(out, contentType);
        } else {
            out.writeBoolean(false);
        }
        out.writeZLong(ifSeqNo);
        out.writeVLong(ifPrimaryTerm);
        out.writeBoolean(requireAlias);
        out.writeMap(dynamicTemplates, StreamOutput::writeString);
        if (out.getTransportVersion().onOrAfter(PIPELINES_HAVE_RUN_FIELD_ADDED)
            && out.getTransportVersion().before(TransportVersions.V_8_13_0)) {
            out.writeBoolean(normalisedBytesParsed != -1L);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeBoolean(listExecutedPipelines);
            if (listExecutedPipelines) {
                out.writeOptionalCollection(executedPipelines, StreamOutput::writeString);
            }
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeBoolean(requireDataStream);
            out.writeZLong(normalisedBytesParsed);
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.INDEX_REQUEST_UPDATE_BY_SCRIPT_ORIGIN)) {
            out.writeBoolean(originatesFromUpdateByScript);
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.INDEX_REQUEST_UPDATE_BY_DOC_ORIGIN)) {
            out.writeBoolean(originatesFromUpdateByDoc);
        }
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            if (source.length() > MAX_SOURCE_LENGTH_IN_TOSTRING) {
                sSource = "n/a, actual length: ["
                    + ByteSizeValue.ofBytes(source.length()).toString()
                    + "], max length: "
                    + ByteSizeValue.ofBytes(MAX_SOURCE_LENGTH_IN_TOSTRING).toString();
            } else {
                sSource = XContentHelper.convertToJson(source, false);
            }
        } catch (Exception e) {
            // ignore
        }
        return "index {[" + index + "][" + id + "], source[" + sSource + "]}";
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * Returns <code>true</code> if this request has been sent to a shard copy more than once.
     */
    public boolean isRetry() {
        return isRetry;
    }

    @Override
    public void onRetry() {
        isRetry = true;
    }

    /**
     * Returns the timestamp the auto generated ID was created or {@value #UNSET_AUTO_GENERATED_TIMESTAMP} if the
     * document has no auto generated timestamp. This method will return a positive value iff the id was auto generated.
     */
    public long getAutoGeneratedTimestamp() {
        return autoGeneratedTimestamp;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(id) + (source == null ? 0 : source.length());
    }

    @Override
    public boolean isRequireAlias() {
        return requireAlias;
    }

    @Override
    public boolean isRequireDataStream() {
        return requireDataStream;
    }

    /**
     * Set whether this IndexRequest requires a data stream. The data stream may be pre-existing or to-be-created.
     */
    public IndexRequest setRequireDataStream(boolean requireDataStream) {
        this.requireDataStream = requireDataStream;
        return this;
    }

    @Override
    public Index getConcreteWriteIndex(IndexAbstraction ia, Metadata metadata) {
        if (DataStream.isFailureStoreFeatureFlagEnabled() && writeToFailureStore) {
            if (ia.isDataStreamRelated() == false) {
                throw new ElasticsearchException(
                    "Attempting to write a document to a failure store but the targeted index is not a data stream"
                );
            }
            // Resolve write index and get parent data stream to handle the case of dealing with an alias
            String defaultWriteIndexName = ia.getWriteIndex().getName();
            DataStream dataStream = metadata.getIndicesLookup().get(defaultWriteIndexName).getParentDataStream();
            if (dataStream.getFailureIndices().getIndices().size() < 1) {
                throw new ElasticsearchException(
                    "Attempting to write a document to a failure store but the target data stream does not have one enabled"
                );
            }
            return dataStream.getFailureIndices().getIndices().get(dataStream.getFailureIndices().getIndices().size() - 1);
        } else {
            // Resolve as normal
            return ia.getWriteIndex(this, metadata);
        }
    }

    @Override
    public int route(IndexRouting indexRouting) {
        return indexRouting.indexShard(id, routing, contentType, source, this::routing);
    }

    public IndexRequest setRequireAlias(boolean requireAlias) {
        this.requireAlias = requireAlias;
        return this;
    }

    public boolean isWriteToFailureStore() {
        return writeToFailureStore;
    }

    public IndexRequest setWriteToFailureStore(boolean writeToFailureStore) {
        this.writeToFailureStore = writeToFailureStore;
        return this;
    }

    public IndexRequest setListExecutedPipelines(boolean listExecutedPipelines) {
        this.listExecutedPipelines = listExecutedPipelines;
        return this;
    }

    public boolean getListExecutedPipelines() {
        return listExecutedPipelines;
    }

    /**
     * Specifies a map from the full path of field names to the name of dynamic mapping templates
     */
    public IndexRequest setDynamicTemplates(Map<String, String> dynamicTemplates) {
        this.dynamicTemplates = Objects.requireNonNull(dynamicTemplates);
        return this;
    }

    /**
     * Returns a map from the full path of field names to the name of dynamic mapping templates.
     *
     * @see #setDynamicTemplates(Map)
     */
    public Map<String, String> getDynamicTemplates() {
        return dynamicTemplates;
    }

    public Object getRawTimestamp() {
        return rawTimestamp;
    }

    public void setRawTimestamp(Object rawTimestamp) {
        assert this.rawTimestamp == null : "rawTimestamp only set in ingest phase, it can't be set twice";
        this.rawTimestamp = rawTimestamp;
    }

    /**
     * Returns a number of bytes observed when parsing a document in earlier stages of ingestion (like update/ingest service)
     * Defaults to -1 when a document size was not observed in earlier stages.
     * @return a number of bytes observed
     */
    public long getNormalisedBytesParsed() {
        return normalisedBytesParsed;
    }

    /**
     * Sets number of bytes observed by a <code>DocumentSizeObserver</code>
     * @return an index request
     */
    public IndexRequest setNormalisedBytesParsed(long normalisedBytesParsed) {
        this.normalisedBytesParsed = normalisedBytesParsed;
        return this;
    }

    /**
     * Adds the pipeline to the list of executed pipelines, if listExecutedPipelines is true
     *
     * @param pipeline
     */
    public void addPipeline(String pipeline) {
        if (listExecutedPipelines) {
            if (executedPipelines == null) {
                executedPipelines = new ArrayList<>();
            }
            executedPipelines.add(pipeline);
        }
    }

    /**
     * This returns the list of pipelines executed on the document for this request. If listExecutedPipelines is false, the response will be
     * null, even if pipelines were executed. If listExecutedPipelines is true but no pipelines were executed, the list will be empty.
     *
     * @return
     */
    @Nullable
    public List<String> getExecutedPipelines() {
        if (listExecutedPipelines == false) {
            return null;
        } else if (executedPipelines == null) { // The client has asked to list pipelines, but none have been executed
            return List.of();
        } else {
            return Collections.unmodifiableList(executedPipelines);
        }
    }

    public IndexRequest setOriginatesFromUpdateByScript(boolean originatesFromUpdateByScript) {
        this.originatesFromUpdateByScript = originatesFromUpdateByScript;
        return this;
    }

    public boolean originatesFromUpdateByScript() {
        return originatesFromUpdateByScript;
    }

    public boolean originatesFromUpdateByDoc() {
        return originatesFromUpdateByDoc;
    }

    public IndexRequest setOriginatesFromUpdateByDoc(boolean originatesFromUpdateByDoc) {
        this.originatesFromUpdateByDoc = originatesFromUpdateByDoc;
        return this;
    }
}
