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
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.admin.indices.alias.delete.AliasesNotFoundException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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

    private static final Map<String, Constructor<? extends ElasticsearchException>> MAPPING;
    private final Map<String, List<String>> headers = new HashMap<>();

    /**
     * Construct a <code>ElasticsearchException</code> with the specified detail message.
     *
     * The message can be parameterized using {@code {}} as placeholders for the given
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
     * The message can be parameterized using {@code {}} as placeholders for the given
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

    public static ElasticsearchException readException(StreamInput input, String name) throws IOException {
        Constructor<? extends ElasticsearchException> elasticsearchException = MAPPING.get(name);
        if (elasticsearchException == null) {
            throw new IllegalStateException("unknown exception with name: " + name);
        }
        try {
            return elasticsearchException.newInstance(input);
        } catch (InstantiationException|IllegalAccessException|InvocationTargetException e) {
            throw new IOException("failed to read exception: [" + name + "]", e);
        }
    }

    /**
     * Retruns <code>true</code> iff the given name is a registered for an exception to be read.
     */
    public static boolean isRegistered(String name) {
        return MAPPING.containsKey(name);
    }

    static Set<String> getRegisteredKeys() { // for testing
        return MAPPING.keySet();
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
    protected final void causeToXContent(XContentBuilder builder, Params params) throws IOException {
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
        Class<? extends ElasticsearchException>[] exceptions = new Class[]{
                org.elasticsearch.common.settings.SettingsException.class,
                org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException.class,
                org.elasticsearch.index.engine.IndexFailedEngineException.class,
                org.elasticsearch.indices.recovery.RecoverFilesRecoveryException.class,
                org.elasticsearch.index.translog.TruncatedTranslogException.class,
                org.elasticsearch.repositories.RepositoryException.class,
                org.elasticsearch.index.engine.DocumentSourceMissingException.class,
                org.elasticsearch.index.engine.DocumentMissingException.class,
                org.elasticsearch.common.util.concurrent.EsRejectedExecutionException.class,
                org.elasticsearch.cluster.routing.RoutingException.class,
                org.elasticsearch.common.lucene.Lucene.EarlyTerminationException.class,
                org.elasticsearch.indices.InvalidAliasNameException.class,
                org.elasticsearch.index.engine.EngineCreationFailureException.class,
                org.elasticsearch.index.snapshots.IndexShardRestoreFailedException.class,
                org.elasticsearch.script.groovy.GroovyScriptCompilationException.class,
                org.elasticsearch.cluster.routing.RoutingValidationException.class,
                org.elasticsearch.snapshots.SnapshotMissingException.class,
                org.elasticsearch.index.shard.IndexShardRecoveryException.class,
                org.elasticsearch.action.search.SearchPhaseExecutionException.class,
                org.elasticsearch.common.util.concurrent.UncategorizedExecutionException.class,
                org.elasticsearch.index.engine.SnapshotFailedEngineException.class,
                org.elasticsearch.action.search.ReduceSearchPhaseException.class,
                org.elasticsearch.action.RoutingMissingException.class,
                org.elasticsearch.index.engine.DeleteFailedEngineException.class,
                org.elasticsearch.indices.recovery.RecoveryFailedException.class,
                org.elasticsearch.search.builder.SearchSourceBuilderException.class,
                org.elasticsearch.index.engine.RefreshFailedEngineException.class,
                org.elasticsearch.index.snapshots.IndexShardSnapshotException.class,
                org.elasticsearch.search.query.QueryPhaseExecutionException.class,
                org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException.class,
                org.elasticsearch.index.percolator.PercolatorException.class,
                org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException.class,
                org.elasticsearch.indices.IndexTemplateAlreadyExistsException.class,
                org.elasticsearch.indices.InvalidIndexNameException.class,
                org.elasticsearch.indices.recovery.DelayRecoveryException.class,
                org.elasticsearch.indices.AliasFilterParsingException.class,
                org.elasticsearch.indices.InvalidIndexTemplateException.class,
                org.elasticsearch.http.HttpException.class,
                org.elasticsearch.index.shard.IndexShardNotRecoveringException.class,
                org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException.class,
                org.elasticsearch.action.UnavailableShardsException.class,
                org.elasticsearch.transport.ActionNotFoundTransportException.class,
                org.elasticsearch.index.shard.TranslogRecoveryPerformer.BatchOperationException.class,
                org.elasticsearch.ElasticsearchException.class,
                org.elasticsearch.index.shard.IndexShardClosedException.class,
                org.elasticsearch.client.transport.NoNodeAvailableException.class,
                org.elasticsearch.cluster.block.ClusterBlockException.class,
                org.elasticsearch.action.FailedNodeException.class,
                org.elasticsearch.indices.TypeMissingException.class,
                org.elasticsearch.indices.InvalidTypeNameException.class,
                org.elasticsearch.transport.netty.SizeHeaderFrameDecoder.HttpOnTransportException.class,
                org.elasticsearch.common.util.CancellableThreads.ExecutionCancelledException.class,
                org.elasticsearch.snapshots.SnapshotCreationException.class,
                org.elasticsearch.script.groovy.GroovyScriptExecutionException.class,
                org.elasticsearch.indices.IndexTemplateMissingException.class,
                org.elasticsearch.transport.NodeNotConnectedException.class,
                org.elasticsearch.index.shard.IndexShardRecoveringException.class,
                org.elasticsearch.index.shard.IndexShardStartedException.class,
                org.elasticsearch.indices.IndexClosedException.class,
                org.elasticsearch.repositories.RepositoryMissingException.class,
                org.elasticsearch.search.warmer.IndexWarmerMissingException.class,
                org.elasticsearch.percolator.PercolateException.class,
                org.elasticsearch.index.engine.EngineException.class,
                org.elasticsearch.script.expression.ExpressionScriptExecutionException.class,
                org.elasticsearch.action.NoShardAvailableActionException.class,
                org.elasticsearch.transport.ReceiveTimeoutTransportException.class,
                org.elasticsearch.http.BindHttpException.class,
                org.elasticsearch.transport.RemoteTransportException.class,
                org.elasticsearch.index.shard.IndexShardRelocatedException.class,
                org.elasticsearch.snapshots.InvalidSnapshotNameException.class,
                org.elasticsearch.repositories.RepositoryVerificationException.class,
                org.elasticsearch.search.SearchException.class,
                org.elasticsearch.transport.ActionTransportException.class,
                org.elasticsearch.common.settings.NoClassSettingsException.class,
                org.elasticsearch.transport.NodeShouldNotConnectException.class,
                org.elasticsearch.index.mapper.MapperParsingException.class,
                org.elasticsearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException.class,
                org.elasticsearch.search.dfs.DfsPhaseExecutionException.class,
                org.elasticsearch.index.engine.VersionConflictEngineException.class,
                org.elasticsearch.snapshots.SnapshotRestoreException.class,
                org.elasticsearch.script.Script.ScriptParseException.class,
                org.elasticsearch.ElasticsearchGenerationException.class,
                org.elasticsearch.action.TimestampParsingException.class,
                org.elasticsearch.action.NoSuchNodeException.class,
                org.elasticsearch.transport.BindTransportException.class,
                org.elasticsearch.snapshots.SnapshotException.class,
                org.elasticsearch.index.mapper.MapperException.class,
                org.elasticsearch.transport.TransportException.class,
                org.elasticsearch.search.SearchContextException.class,
                org.elasticsearch.index.translog.TranslogCorruptedException.class,
                org.elasticsearch.transport.TransportSerializationException.class,
                org.elasticsearch.cluster.IncompatibleClusterStateVersionException.class,
                org.elasticsearch.indices.IndexCreationException.class,
                org.elasticsearch.index.mapper.MergeMappingException.class,
                org.elasticsearch.transport.NotSerializableTransportException.class,
                org.elasticsearch.ElasticsearchTimeoutException.class,
                org.elasticsearch.search.SearchContextMissingException.class,
                org.elasticsearch.transport.SendRequestTransportException.class,
                org.elasticsearch.index.IndexShardAlreadyExistsException.class,
                org.elasticsearch.indices.IndexAlreadyExistsException.class,
                org.elasticsearch.index.engine.DocumentAlreadyExistsException.class,
                org.elasticsearch.transport.ConnectTransportException.class,
                org.elasticsearch.gateway.GatewayException.class,
                org.elasticsearch.script.ScriptException.class,
                org.elasticsearch.script.expression.ExpressionScriptCompilationException.class,
                org.elasticsearch.index.shard.IndexShardNotStartedException.class,
                org.elasticsearch.index.mapper.StrictDynamicMappingException.class,
                org.elasticsearch.index.engine.EngineClosedException.class,
                AliasesNotFoundException.class,
                org.elasticsearch.transport.ResponseHandlerFailureTransportException.class,
                org.elasticsearch.search.SearchParseException.class,
                org.elasticsearch.search.fetch.FetchPhaseExecutionException.class,
                org.elasticsearch.transport.NodeDisconnectedException.class,
                org.elasticsearch.common.breaker.CircuitBreakingException.class,
                org.elasticsearch.search.aggregations.AggregationInitializationException.class,
                org.elasticsearch.search.aggregations.InvalidAggregationPathException.class,
                org.elasticsearch.cluster.routing.IllegalShardRoutingStateException.class,
                org.elasticsearch.index.engine.FlushFailedEngineException.class,
                org.elasticsearch.index.AlreadyExpiredException.class,
                org.elasticsearch.index.translog.TranslogException.class,
                org.elasticsearch.index.engine.FlushNotAllowedEngineException.class,
                org.elasticsearch.index.engine.RecoveryEngineException.class,
                org.elasticsearch.common.blobstore.BlobStoreException.class,
                org.elasticsearch.index.snapshots.IndexShardRestoreException.class,
                org.elasticsearch.index.query.QueryParsingException.class,
                org.elasticsearch.action.support.replication.TransportReplicationAction.RetryOnPrimaryException.class,
                org.elasticsearch.index.engine.DeleteByQueryFailedEngineException.class,
                org.elasticsearch.discovery.MasterNotDiscoveredException.class,
                org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException.class,
                org.elasticsearch.node.NodeClosedException.class,
                org.elasticsearch.search.aggregations.AggregationExecutionException.class,
                org.elasticsearch.ElasticsearchParseException.class,
                org.elasticsearch.action.PrimaryMissingActionException.class,
                org.elasticsearch.index.engine.CreateFailedEngineException.class,
                org.elasticsearch.index.shard.IllegalIndexShardStateException.class,
                ElasticsearchSecurityException.class,
                ResourceNotFoundException.class,
                IndexNotFoundException.class,
                ShardNotFoundException.class,
                NotSerializableExceptionWrapper.class,
                Discovery.FailedToCommitClusterStateException.class
        };
        Map<String, Constructor<? extends ElasticsearchException>> mapping = new HashMap<>(exceptions.length);
        for (Class<? extends ElasticsearchException> e : exceptions) {
            String name = e.getName();
            try {
                Constructor<? extends ElasticsearchException> constructor = e.getDeclaredConstructor(StreamInput.class);
                if (constructor == null) {
                    throw new IllegalStateException(name + " has not StreamInput ctor");
                }
                mapping.put(name, constructor);
            } catch (NoSuchMethodException t) {
                throw new RuntimeException("failed to register [" + name + "] exception must have a public StreamInput ctor", t);
            }
        }

        MAPPING = Collections.unmodifiableMap(mapping);
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
}
