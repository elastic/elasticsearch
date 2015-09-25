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

package org.elasticsearch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.*;

/**
 * A base class for all elasticsearch exceptions.
 */
public class ElasticsearchException extends RuntimeException implements ToXContent {

    public static final String REST_EXCEPTION_SKIP_CAUSE = "rest.exception.cause.skip";
    public static final String REST_EXCEPTION_SKIP_STACK_TRACE = "rest.exception.stacktrace.skip";
    public static final boolean REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT = true;
    public static final boolean REST_EXCEPTION_SKIP_CAUSE_DEFAULT = false;
    private static final String INDEX_HEADER_KEY = "es.index";
    private static final String SHARD_HEADER_KEY = "es.shard";
    private static final String RESOURCE_HEADER_TYPE_KEY = "es.resource.type";
    private static final String RESOURCE_HEADER_ID_KEY = "es.resource.id";

    private static final IOFunction<StreamInput, ? extends ElasticsearchException>[] ID_TO_SUPPLIER;
    private static final Map<Class<? extends ElasticsearchException>, IdAndCtor> CLASS_TO_ID;
    private final Map<String, List<String>> headers = new HashMap<>();

    /**
     * Construct a <code>ElasticsearchException</code> with the specified detail message.
     *
     * The message can be parameterized using <code>{}</code> as placeholders for the given
     * arguments
     *
     * @param msg the detail message
     * @param args the arguments for the message
     */
    public ElasticsearchException(String msg, Object... args) {
        super(LoggerMessageFormat.format(msg, args));
    }

    /**
     * Construct a <code>ElasticsearchException</code> with the specified detail message
     * and nested exception.
     *
     * The message can be parameterized using <code>{}</code> as placeholders for the given
     * arguments
     *
     * @param msg   the detail message
     * @param cause the nested exception
     * @param args  the arguments for the message
     */
    public ElasticsearchException(String msg, Throwable cause, Object... args) {
        super(LoggerMessageFormat.format(msg, args), cause);
    }

    public ElasticsearchException(StreamInput in) throws IOException {
        super(in.readOptionalString(), in.readThrowable());
        readStackTrace(this, in);
        int numKeys = in.readVInt();
        for (int i = 0; i < numKeys; i++) {
            final String key = in.readString();
            final int numValues = in.readVInt();
            final ArrayList<String> values = new ArrayList<>(numValues);
            for (int j = 0; j < numValues; j++) {
                values.add(in.readString());
            }
            headers.put(key, values);
        }
    }

    /**
     * Adds a new header with the given key.
     * This method will replace existing header if a header with the same key already exists
     */
    public void addHeader(String key, String... value) {
        this.headers.put(key, Arrays.asList(value));
    }

    /**
     * Adds a new header with the given key.
     * This method will replace existing header if a header with the same key already exists
     */
    public void addHeader(String key, List<String> value) {
        this.headers.put(key, value);
    }


    /**
     * Returns a set of all header keys on this exception
     */
    public Set<String> getHeaderKeys() {
        return headers.keySet();
    }

    /**
     * Returns the list of header values for the given key or {@code null} if not header for the
     * given key exists.
     */
    public List<String> getHeader(String key) {
        return headers.get(key);
    }

    /**
     * Returns the rest status code associated with this exception.
     */
    public RestStatus status() {
        Throwable cause = unwrapCause();
        if (cause == this) {
            return RestStatus.INTERNAL_SERVER_ERROR;
        } else {
            return ExceptionsHelper.status(cause);
        }
    }

    /**
     * Unwraps the actual cause from the exception for cases when the exception is a
     * {@link ElasticsearchWrapperException}.
     *
     * @see org.elasticsearch.ExceptionsHelper#unwrapCause(Throwable)
     */
    public Throwable unwrapCause() {
        return ExceptionsHelper.unwrapCause(this);
    }

    /**
     * Return the detail message, including the message from the nested exception
     * if there is one.
     */
    public String getDetailedMessage() {
        if (getCause() != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(toString()).append("; ");
            if (getCause() instanceof ElasticsearchException) {
                sb.append(((ElasticsearchException) getCause()).getDetailedMessage());
            } else {
                sb.append(getCause());
            }
            return sb.toString();
        } else {
            return super.toString();
        }
    }


    /**
     * Retrieve the innermost cause of this exception, if none, returns the current exception.
     */
    public Throwable getRootCause() {
        Throwable rootCause = this;
        Throwable cause = getCause();
        while (cause != null && cause != rootCause) {
            rootCause = cause;
            cause = cause.getCause();
        }
        return rootCause;
    }

    /**
     * Check whether this exception contains an exception of the given type:
     * either it is of the given class itself or it contains a nested cause
     * of the given type.
     *
     * @param exType the exception type to look for
     * @return whether there is a nested exception of the specified type
     */
    public boolean contains(Class exType) {
        if (exType == null) {
            return false;
        }
        if (exType.isInstance(this)) {
            return true;
        }
        Throwable cause = getCause();
        if (cause == this) {
            return false;
        }
        if (cause instanceof ElasticsearchException) {
            return ((ElasticsearchException) cause).contains(exType);
        } else {
            while (cause != null) {
                if (exType.isInstance(cause)) {
                    return true;
                }
                if (cause.getCause() == cause) {
                    break;
                }
                cause = cause.getCause();
            }
            return false;
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(this.getMessage());
        out.writeThrowable(this.getCause());
        writeStackTraces(this, out);
        out.writeVInt(headers.size());
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue().size());
            for (String v : entry.getValue()) {
                out.writeString(v);
            }
        }
    }

    public static ElasticsearchException readException(StreamInput input, int id) throws IOException {
        IOFunction<StreamInput, ? extends ElasticsearchException> elasticsearchException = ID_TO_SUPPLIER[id];
        if (elasticsearchException == null) {
            throw new IllegalStateException("unknown exception for id: " + id);
        }
        return elasticsearchException.apply(input);
    }

    /**
     * Retruns <code>true</code> iff the given class is a registered for an exception to be read.
     */
    public static boolean isRegistered(Class<? extends Throwable> exception) {
        return CLASS_TO_ID.containsKey(exception);
    }

    static Set<Class<? extends ElasticsearchException>> getRegisteredKeys() { // for testing
        return CLASS_TO_ID.keySet();
    }

    /**
     * Returns the serialization id the given exception.
     */
    public static int getId(Class<? extends ElasticsearchException> exception) {
        return CLASS_TO_ID.get(exception).id;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Throwable ex = ExceptionsHelper.unwrapCause(this);
        if (ex != this) {
            toXContent(builder, params, this);
        } else {
            builder.field("type", getExceptionName());
            builder.field("reason", getMessage());
            for (String key : headers.keySet()) {
                if (key.startsWith("es.")) {
                    List<String> values = headers.get(key);
                    xContentHeader(builder, key.substring("es.".length()), values);
                }
            }
            innerToXContent(builder, params);
            renderHeader(builder, params);
            if (params.paramAsBoolean(REST_EXCEPTION_SKIP_STACK_TRACE, REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT) == false) {
                builder.field("stack_trace", ExceptionsHelper.stackTrace(this));
            }
        }
        return builder;
    }

    /**
     * Renders additional per exception information into the xcontent
     */
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        causeToXContent(builder, params);
    }

    /**
     * Renders a cause exception as xcontent
     */
    protected void causeToXContent(XContentBuilder builder, Params params) throws IOException {
        final Throwable cause = getCause();
        if (cause != null && params.paramAsBoolean(REST_EXCEPTION_SKIP_CAUSE, REST_EXCEPTION_SKIP_CAUSE_DEFAULT) == false) {
            builder.field("caused_by");
            builder.startObject();
            toXContent(builder, params, cause);
            builder.endObject();
        }
    }

    protected final void renderHeader(XContentBuilder builder, Params params) throws IOException {
        boolean hasHeader = false;
        for (String key : headers.keySet()) {
            if (key.startsWith("es.")) {
                continue;
            }
            if (hasHeader == false) {
                builder.startObject("header");
                hasHeader = true;
            }
            List<String> values = headers.get(key);
            xContentHeader(builder, key, values);
        }
        if (hasHeader) {
            builder.endObject();
        }
    }

    private void xContentHeader(XContentBuilder builder, String key, List<String> values) throws IOException {
        if (values != null && values.isEmpty() == false) {
            if(values.size() == 1) {
                builder.field(key, values.get(0));
            } else {
                builder.startArray(key);
                for (String value : values) {
                    builder.value(value);
                }
                builder.endArray();
            }
        }
    }

    /**
     * Statis toXContent helper method that also renders non {@link org.elasticsearch.ElasticsearchException} instances as XContent.
     */
    public static void toXContent(XContentBuilder builder, Params params, Throwable ex) throws IOException {
        ex = ExceptionsHelper.unwrapCause(ex);
        if (ex instanceof ElasticsearchException) {
            ((ElasticsearchException) ex).toXContent(builder, params);
        } else {
            builder.field("type", getExceptionName(ex));
            builder.field("reason", ex.getMessage());
            if (ex.getCause() != null) {
                builder.field("caused_by");
                builder.startObject();
                toXContent(builder, params, ex.getCause());
                builder.endObject();
            }
            if (params.paramAsBoolean(REST_EXCEPTION_SKIP_STACK_TRACE, REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT) == false) {
                builder.field("stack_trace", ExceptionsHelper.stackTrace(ex));
            }
        }
    }

    /**
     * Returns the root cause of this exception or mupltiple if different shards caused different exceptions
     */
    public ElasticsearchException[] guessRootCauses() {
        final Throwable cause = getCause();
        if (cause != null && cause instanceof ElasticsearchException) {
            return ((ElasticsearchException) cause).guessRootCauses();
        }
        return new ElasticsearchException[] {this};
    }

    /**
     * Returns the root cause of this exception or mupltiple if different shards caused different exceptions.
     * If the given exception is not an instance of {@link org.elasticsearch.ElasticsearchException} an empty array
     * is returned.
     */
    public static ElasticsearchException[] guessRootCauses(Throwable t) {
        Throwable ex = ExceptionsHelper.unwrapCause(t);
        if (ex instanceof ElasticsearchException) {
            return ((ElasticsearchException) ex).guessRootCauses();
        }
        return new ElasticsearchException[] {new ElasticsearchException(t.getMessage(), t) {
            @Override
            protected String getExceptionName() {
                return getExceptionName(getCause());
            }
        }};
    }

    protected String getExceptionName() {
        return getExceptionName(this);
    }

    /**
     * Returns a underscore case name for the given exception. This method strips <tt>Elasticsearch</tt> prefixes from exception names.
     */
    public static String getExceptionName(Throwable ex) {
        String simpleName = ex.getClass().getSimpleName();
        if (simpleName.startsWith("Elasticsearch")) {
            simpleName = simpleName.substring("Elasticsearch".length());
        }
        return Strings.toUnderscoreCase(simpleName);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (headers.containsKey(INDEX_HEADER_KEY)) {
            builder.append('[').append(getIndex()).append(']');
            if (headers.containsKey(SHARD_HEADER_KEY)) {
                builder.append('[').append(getShardId()).append(']');
            }
            builder.append(' ');
        }
        return builder.append(ExceptionsHelper.detailedMessage(this).trim()).toString();
    }

    /**
     * Deserializes stacktrace elements as well as suppressed exceptions from the given output stream and
     * adds it to the given exception.
     */
    public static <T extends Throwable> T readStackTrace(T throwable, StreamInput in) throws IOException {
        final int stackTraceElements = in.readVInt();
        StackTraceElement[] stackTrace = new StackTraceElement[stackTraceElements];
        for (int i = 0; i < stackTraceElements; i++) {
            final String declaringClasss = in.readString();
            final String fileName = in.readOptionalString();
            final String methodName = in.readString();
            final int lineNumber = in.readVInt();
            stackTrace[i] = new StackTraceElement(declaringClasss,methodName, fileName, lineNumber);
        }
        throwable.setStackTrace(stackTrace);

        int numSuppressed = in.readVInt();
        for (int i = 0; i < numSuppressed; i++) {
            throwable.addSuppressed(in.readThrowable());
        }
        return throwable;
    }

    /**
     * Serializes the given exceptions stacktrace elements as well as it's suppressed exceptions to the given output stream.
     */
    public static <T extends Throwable> T writeStackTraces(T throwable, StreamOutput out) throws IOException {
        StackTraceElement[] stackTrace = throwable.getStackTrace();
        out.writeVInt(stackTrace.length);
        for (StackTraceElement element : stackTrace) {
            out.writeString(element.getClassName());
            out.writeOptionalString(element.getFileName());
            out.writeString(element.getMethodName());
            out.writeVInt(element.getLineNumber());
        }
        Throwable[] suppressed = throwable.getSuppressed();
        out.writeVInt(suppressed.length);
        for (Throwable t : suppressed) {
            out.writeThrowable(t);
        }
        return throwable;
    }

    static {
        // each exception gets an ordinal assigned that must never change. While the exception name can
        // change due to refactorings etc. like renaming we have to keep the ordinal <--> class mapping
        // to deserialize the exception coming from another node or from an corruption marker on
        // a corrupted index.
        // NOTE: ONLY APPEND TO THE END and NEVER REMOVE EXCEPTIONS IN MINOR VERSIONS
        final Map<Class<? extends ElasticsearchException>, IdAndCtor> exceptions = new HashMap<>();
        exceptions.put(org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException.class, new IdAndCtor(org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.search.dfs.DfsPhaseExecutionException.class, new IdAndCtor(org.elasticsearch.search.dfs.DfsPhaseExecutionException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.common.util.CancellableThreads.ExecutionCancelledException.class, new IdAndCtor(org.elasticsearch.common.util.CancellableThreads.ExecutionCancelledException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.discovery.MasterNotDiscoveredException.class, new IdAndCtor(org.elasticsearch.discovery.MasterNotDiscoveredException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.ElasticsearchSecurityException.class, new IdAndCtor(org.elasticsearch.ElasticsearchSecurityException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.snapshots.IndexShardRestoreException.class, new IdAndCtor(org.elasticsearch.index.snapshots.IndexShardRestoreException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.IndexClosedException.class, new IdAndCtor(org.elasticsearch.indices.IndexClosedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.http.BindHttpException.class, new IdAndCtor(org.elasticsearch.http.BindHttpException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.action.search.ReduceSearchPhaseException.class, new IdAndCtor(org.elasticsearch.action.search.ReduceSearchPhaseException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.node.NodeClosedException.class, new IdAndCtor(org.elasticsearch.node.NodeClosedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.SnapshotFailedEngineException.class, new IdAndCtor(org.elasticsearch.index.engine.SnapshotFailedEngineException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.shard.ShardNotFoundException.class, new IdAndCtor(org.elasticsearch.index.shard.ShardNotFoundException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.ConnectTransportException.class, new IdAndCtor(org.elasticsearch.transport.ConnectTransportException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.NotSerializableTransportException.class, new IdAndCtor(org.elasticsearch.transport.NotSerializableTransportException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.ResponseHandlerFailureTransportException.class, new IdAndCtor(org.elasticsearch.transport.ResponseHandlerFailureTransportException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.IndexCreationException.class, new IdAndCtor(org.elasticsearch.indices.IndexCreationException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.IndexNotFoundException.class, new IdAndCtor(org.elasticsearch.index.IndexNotFoundException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.cluster.routing.IllegalShardRoutingStateException.class, new IdAndCtor(org.elasticsearch.cluster.routing.IllegalShardRoutingStateException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException.class, new IdAndCtor(org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.ResourceNotFoundException.class, new IdAndCtor(org.elasticsearch.ResourceNotFoundException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.ActionTransportException.class, new IdAndCtor(org.elasticsearch.transport.ActionTransportException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.ElasticsearchGenerationException.class, new IdAndCtor(org.elasticsearch.ElasticsearchGenerationException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.CreateFailedEngineException.class, new IdAndCtor(org.elasticsearch.index.engine.CreateFailedEngineException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.shard.IndexShardStartedException.class, new IdAndCtor(org.elasticsearch.index.shard.IndexShardStartedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.search.SearchContextMissingException.class, new IdAndCtor(org.elasticsearch.search.SearchContextMissingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.script.ScriptException.class, new IdAndCtor(org.elasticsearch.script.ScriptException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.shard.TranslogRecoveryPerformer.BatchOperationException.class, new IdAndCtor(org.elasticsearch.index.shard.TranslogRecoveryPerformer.BatchOperationException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.snapshots.SnapshotCreationException.class, new IdAndCtor(org.elasticsearch.snapshots.SnapshotCreationException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.DeleteFailedEngineException.class, new IdAndCtor(org.elasticsearch.index.engine.DeleteFailedEngineException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.DocumentMissingException.class, new IdAndCtor(org.elasticsearch.index.engine.DocumentMissingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.snapshots.SnapshotException.class, new IdAndCtor(org.elasticsearch.snapshots.SnapshotException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.InvalidAliasNameException.class, new IdAndCtor(org.elasticsearch.indices.InvalidAliasNameException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.InvalidIndexNameException.class, new IdAndCtor(org.elasticsearch.indices.InvalidIndexNameException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException.class, new IdAndCtor(org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.TransportException.class, new IdAndCtor(org.elasticsearch.transport.TransportException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.ElasticsearchParseException.class, new IdAndCtor(org.elasticsearch.ElasticsearchParseException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.search.SearchException.class, new IdAndCtor(org.elasticsearch.search.SearchException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.mapper.MapperException.class, new IdAndCtor(org.elasticsearch.index.mapper.MapperException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.InvalidTypeNameException.class, new IdAndCtor(org.elasticsearch.indices.InvalidTypeNameException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.snapshots.SnapshotRestoreException.class, new IdAndCtor(org.elasticsearch.snapshots.SnapshotRestoreException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.common.ParsingException.class, new IdAndCtor(org.elasticsearch.common.ParsingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.shard.IndexShardClosedException.class, new IdAndCtor(org.elasticsearch.index.shard.IndexShardClosedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.recovery.RecoverFilesRecoveryException.class, new IdAndCtor(org.elasticsearch.indices.recovery.RecoverFilesRecoveryException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.translog.TruncatedTranslogException.class, new IdAndCtor(org.elasticsearch.index.translog.TruncatedTranslogException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.recovery.RecoveryFailedException.class, new IdAndCtor(org.elasticsearch.indices.recovery.RecoveryFailedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.shard.IndexShardRelocatedException.class, new IdAndCtor(org.elasticsearch.index.shard.IndexShardRelocatedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.NodeShouldNotConnectException.class, new IdAndCtor(org.elasticsearch.transport.NodeShouldNotConnectException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.IndexTemplateAlreadyExistsException.class, new IdAndCtor(org.elasticsearch.indices.IndexTemplateAlreadyExistsException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.translog.TranslogCorruptedException.class, new IdAndCtor(org.elasticsearch.index.translog.TranslogCorruptedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.cluster.block.ClusterBlockException.class, new IdAndCtor(org.elasticsearch.cluster.block.ClusterBlockException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.search.fetch.FetchPhaseExecutionException.class, new IdAndCtor(org.elasticsearch.search.fetch.FetchPhaseExecutionException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.IndexShardAlreadyExistsException.class, new IdAndCtor(org.elasticsearch.index.IndexShardAlreadyExistsException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.VersionConflictEngineException.class, new IdAndCtor(org.elasticsearch.index.engine.VersionConflictEngineException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.EngineException.class, new IdAndCtor(org.elasticsearch.index.engine.EngineException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.DocumentAlreadyExistsException.class, new IdAndCtor(org.elasticsearch.index.engine.DocumentAlreadyExistsException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.action.NoSuchNodeException.class, new IdAndCtor(org.elasticsearch.action.NoSuchNodeException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.common.settings.SettingsException.class, new IdAndCtor(org.elasticsearch.common.settings.SettingsException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.IndexTemplateMissingException.class, new IdAndCtor(org.elasticsearch.indices.IndexTemplateMissingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.SendRequestTransportException.class, new IdAndCtor(org.elasticsearch.transport.SendRequestTransportException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.common.util.concurrent.EsRejectedExecutionException.class, new IdAndCtor(org.elasticsearch.common.util.concurrent.EsRejectedExecutionException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.common.lucene.Lucene.EarlyTerminationException.class, new IdAndCtor(org.elasticsearch.common.lucene.Lucene.EarlyTerminationException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.cluster.routing.RoutingValidationException.class, new IdAndCtor(org.elasticsearch.cluster.routing.RoutingValidationException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper.class, new IdAndCtor(org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.AliasFilterParsingException.class, new IdAndCtor(org.elasticsearch.indices.AliasFilterParsingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.DeleteByQueryFailedEngineException.class, new IdAndCtor(org.elasticsearch.index.engine.DeleteByQueryFailedEngineException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.gateway.GatewayException.class, new IdAndCtor(org.elasticsearch.gateway.GatewayException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.shard.IndexShardNotRecoveringException.class, new IdAndCtor(org.elasticsearch.index.shard.IndexShardNotRecoveringException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.http.HttpException.class, new IdAndCtor(org.elasticsearch.http.HttpException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.ElasticsearchException.class, new IdAndCtor(org.elasticsearch.ElasticsearchException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.snapshots.SnapshotMissingException.class, new IdAndCtor(org.elasticsearch.snapshots.SnapshotMissingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.action.PrimaryMissingActionException.class, new IdAndCtor(org.elasticsearch.action.PrimaryMissingActionException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.action.FailedNodeException.class, new IdAndCtor(org.elasticsearch.action.FailedNodeException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.search.SearchParseException.class, new IdAndCtor(org.elasticsearch.search.SearchParseException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException.class, new IdAndCtor(org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.common.blobstore.BlobStoreException.class, new IdAndCtor(org.elasticsearch.common.blobstore.BlobStoreException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.cluster.IncompatibleClusterStateVersionException.class, new IdAndCtor(org.elasticsearch.cluster.IncompatibleClusterStateVersionException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.RecoveryEngineException.class, new IdAndCtor(org.elasticsearch.index.engine.RecoveryEngineException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.common.util.concurrent.UncategorizedExecutionException.class, new IdAndCtor(org.elasticsearch.common.util.concurrent.UncategorizedExecutionException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.action.TimestampParsingException.class, new IdAndCtor(org.elasticsearch.action.TimestampParsingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.action.RoutingMissingException.class, new IdAndCtor(org.elasticsearch.action.RoutingMissingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.IndexFailedEngineException.class, new IdAndCtor(org.elasticsearch.index.engine.IndexFailedEngineException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.snapshots.IndexShardRestoreFailedException.class, new IdAndCtor(org.elasticsearch.index.snapshots.IndexShardRestoreFailedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.repositories.RepositoryException.class, new IdAndCtor(org.elasticsearch.repositories.RepositoryException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.ReceiveTimeoutTransportException.class, new IdAndCtor(org.elasticsearch.transport.ReceiveTimeoutTransportException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.NodeDisconnectedException.class, new IdAndCtor(org.elasticsearch.transport.NodeDisconnectedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.AlreadyExpiredException.class, new IdAndCtor(org.elasticsearch.index.AlreadyExpiredException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.search.aggregations.AggregationExecutionException.class, new IdAndCtor(org.elasticsearch.search.aggregations.AggregationExecutionException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.mapper.MergeMappingException.class, new IdAndCtor(org.elasticsearch.index.mapper.MergeMappingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.InvalidIndexTemplateException.class, new IdAndCtor(org.elasticsearch.indices.InvalidIndexTemplateException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.percolator.PercolateException.class, new IdAndCtor(org.elasticsearch.percolator.PercolateException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.RefreshFailedEngineException.class, new IdAndCtor(org.elasticsearch.index.engine.RefreshFailedEngineException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.search.aggregations.AggregationInitializationException.class, new IdAndCtor(org.elasticsearch.search.aggregations.AggregationInitializationException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.recovery.DelayRecoveryException.class, new IdAndCtor(org.elasticsearch.indices.recovery.DelayRecoveryException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.search.warmer.IndexWarmerMissingException.class, new IdAndCtor(org.elasticsearch.search.warmer.IndexWarmerMissingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.client.transport.NoNodeAvailableException.class, new IdAndCtor(org.elasticsearch.client.transport.NoNodeAvailableException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.script.groovy.GroovyScriptCompilationException.class, new IdAndCtor(org.elasticsearch.script.groovy.GroovyScriptCompilationException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.snapshots.InvalidSnapshotNameException.class, new IdAndCtor(org.elasticsearch.snapshots.InvalidSnapshotNameException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.shard.IllegalIndexShardStateException.class, new IdAndCtor(org.elasticsearch.index.shard.IllegalIndexShardStateException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.snapshots.IndexShardSnapshotException.class, new IdAndCtor(org.elasticsearch.index.snapshots.IndexShardSnapshotException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.shard.IndexShardNotStartedException.class, new IdAndCtor(org.elasticsearch.index.shard.IndexShardNotStartedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.action.search.SearchPhaseExecutionException.class, new IdAndCtor(org.elasticsearch.action.search.SearchPhaseExecutionException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.ActionNotFoundTransportException.class, new IdAndCtor(org.elasticsearch.transport.ActionNotFoundTransportException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.TransportSerializationException.class, new IdAndCtor(org.elasticsearch.transport.TransportSerializationException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.RemoteTransportException.class, new IdAndCtor(org.elasticsearch.transport.RemoteTransportException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.EngineCreationFailureException.class, new IdAndCtor(org.elasticsearch.index.engine.EngineCreationFailureException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.cluster.routing.RoutingException.class, new IdAndCtor(org.elasticsearch.cluster.routing.RoutingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.shard.IndexShardRecoveryException.class, new IdAndCtor(org.elasticsearch.index.shard.IndexShardRecoveryException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.repositories.RepositoryMissingException.class, new IdAndCtor(org.elasticsearch.repositories.RepositoryMissingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.percolator.PercolatorException.class, new IdAndCtor(org.elasticsearch.index.percolator.PercolatorException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.DocumentSourceMissingException.class, new IdAndCtor(org.elasticsearch.index.engine.DocumentSourceMissingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.FlushNotAllowedEngineException.class, new IdAndCtor(org.elasticsearch.index.engine.FlushNotAllowedEngineException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.common.settings.NoClassSettingsException.class, new IdAndCtor(org.elasticsearch.common.settings.NoClassSettingsException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.BindTransportException.class, new IdAndCtor(org.elasticsearch.transport.BindTransportException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.rest.action.admin.indices.alias.delete.AliasesNotFoundException.class, new IdAndCtor(org.elasticsearch.rest.action.admin.indices.alias.delete.AliasesNotFoundException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.shard.IndexShardRecoveringException.class, new IdAndCtor(org.elasticsearch.index.shard.IndexShardRecoveringException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.translog.TranslogException.class, new IdAndCtor(org.elasticsearch.index.translog.TranslogException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException.class, new IdAndCtor(org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.action.support.replication.TransportReplicationAction.RetryOnPrimaryException.class, new IdAndCtor(org.elasticsearch.action.support.replication.TransportReplicationAction.RetryOnPrimaryException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.ElasticsearchTimeoutException.class, new IdAndCtor(org.elasticsearch.ElasticsearchTimeoutException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.search.query.QueryPhaseExecutionException.class, new IdAndCtor(org.elasticsearch.search.query.QueryPhaseExecutionException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.repositories.RepositoryVerificationException.class, new IdAndCtor(org.elasticsearch.repositories.RepositoryVerificationException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.search.aggregations.InvalidAggregationPathException.class, new IdAndCtor(org.elasticsearch.search.aggregations.InvalidAggregationPathException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.script.groovy.GroovyScriptExecutionException.class, new IdAndCtor(org.elasticsearch.script.groovy.GroovyScriptExecutionException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.IndexAlreadyExistsException.class, new IdAndCtor(org.elasticsearch.indices.IndexAlreadyExistsException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.script.Script.ScriptParseException.class, new IdAndCtor(org.elasticsearch.script.Script.ScriptParseException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.netty.SizeHeaderFrameDecoder.HttpOnTransportException.class, new IdAndCtor(org.elasticsearch.transport.netty.SizeHeaderFrameDecoder.HttpOnTransportException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.mapper.MapperParsingException.class, new IdAndCtor(org.elasticsearch.index.mapper.MapperParsingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.search.SearchContextException.class, new IdAndCtor(org.elasticsearch.search.SearchContextException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.search.builder.SearchSourceBuilderException.class, new IdAndCtor(org.elasticsearch.search.builder.SearchSourceBuilderException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.EngineClosedException.class, new IdAndCtor(org.elasticsearch.index.engine.EngineClosedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.action.NoShardAvailableActionException.class, new IdAndCtor(org.elasticsearch.action.NoShardAvailableActionException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.action.UnavailableShardsException.class, new IdAndCtor(org.elasticsearch.action.UnavailableShardsException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.engine.FlushFailedEngineException.class, new IdAndCtor(org.elasticsearch.index.engine.FlushFailedEngineException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.common.breaker.CircuitBreakingException.class, new IdAndCtor(org.elasticsearch.common.breaker.CircuitBreakingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.transport.NodeNotConnectedException.class, new IdAndCtor(org.elasticsearch.transport.NodeNotConnectedException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.mapper.StrictDynamicMappingException.class, new IdAndCtor(org.elasticsearch.index.mapper.StrictDynamicMappingException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException.class, new IdAndCtor(org.elasticsearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.indices.TypeMissingException.class, new IdAndCtor(org.elasticsearch.indices.TypeMissingException::new, exceptions.size()));
        // added in 3.x
        exceptions.put(org.elasticsearch.discovery.Discovery.FailedToCommitClusterStateException.class, new IdAndCtor(org.elasticsearch.discovery.Discovery.FailedToCommitClusterStateException::new, exceptions.size()));
        exceptions.put(org.elasticsearch.index.query.QueryShardException.class, new IdAndCtor(org.elasticsearch.index.query.QueryShardException::new, exceptions.size()));
        // NOTE: ONLY APPEND TO THE END and NEVER REMOVE EXCEPTIONS IN MINOR VERSIONS
        IOFunction<StreamInput, ? extends ElasticsearchException>[] idToSupplier = new IOFunction[exceptions.size()];
        for (Map.Entry<Class<? extends ElasticsearchException>, IdAndCtor> e : exceptions.entrySet()) {
            IdAndCtor value = e.getValue();
            assert value.id >= 0;
            if (idToSupplier[value.id] != null) {
                throw new IllegalStateException("ordinal [" + value.id +"] is used more than once");
            }
            idToSupplier[value.id] = value.ctor;
        }
        for (int i = 0; i < idToSupplier.length; i++) {
            if (idToSupplier[i] == null) {
                throw new IllegalStateException("missing exception for ordinal [" + i + "]");
            }
        }

        ID_TO_SUPPLIER = idToSupplier;
        CLASS_TO_ID = Collections.unmodifiableMap(exceptions);
    }

    public String getIndex() {
        List<String> index = getHeader(INDEX_HEADER_KEY);
        if (index != null && index.isEmpty() == false) {
            return index.get(0);
        }

        return null;
    }

    public ShardId getShardId() {
        List<String> shard = getHeader(SHARD_HEADER_KEY);
        if (shard != null && shard.isEmpty() == false) {
            return new ShardId(getIndex(), Integer.parseInt(shard.get(0)));
        }
        return null;
    }

    public void setIndex(Index index) {
        if (index != null) {
            addHeader(INDEX_HEADER_KEY, index.getName());
        }
    }

    public void setIndex(String index) {
        if (index != null) {
            addHeader(INDEX_HEADER_KEY, index);
        }
    }

    public void setShard(ShardId shardId) {
        if (shardId != null) {
            addHeader(INDEX_HEADER_KEY, shardId.getIndex());
            addHeader(SHARD_HEADER_KEY, Integer.toString(shardId.id()));
        }
    }

    public void setResources(String type, String... id) {
        assert type != null;
        addHeader(RESOURCE_HEADER_ID_KEY, id);
        addHeader(RESOURCE_HEADER_TYPE_KEY, type);
    }

    public List<String> getResourceId() {
        return getHeader(RESOURCE_HEADER_ID_KEY);
    }

    public String getResourceType() {
        List<String> header = getHeader(RESOURCE_HEADER_TYPE_KEY);
        if (header != null && header.isEmpty() == false) {
            assert header.size() == 1;
            return header.get(0);
        }
        return null;
    }

    public static void renderThrowable(XContentBuilder builder, Params params, Throwable t) throws IOException {
        builder.startObject("error");
        final ElasticsearchException[] rootCauses = ElasticsearchException.guessRootCauses(t);
        builder.field("root_cause");
        builder.startArray();
        for (ElasticsearchException rootCause : rootCauses){
            builder.startObject();
            rootCause.toXContent(builder, new ToXContent.DelegatingMapParams(Collections.singletonMap(ElasticsearchException.REST_EXCEPTION_SKIP_CAUSE, "true"), params));
            builder.endObject();
        }
        builder.endArray();
        ElasticsearchException.toXContent(builder, params, t);
        builder.endObject();
    }

    interface IOFunction<T, R> {

        /**
         * Applies this function to the given argument.
         *
         * @param t the function argument
         * @return the function result
         */
        R apply(T t) throws IOException;
    }

    static class IdAndCtor {
        final IOFunction<StreamInput, ? extends ElasticsearchException> ctor;
        final int id;

        IdAndCtor(IOFunction<StreamInput, ? extends ElasticsearchException> ctor, int id) {
            this.ctor = ctor;
            this.id = id;
        }
    }
}
