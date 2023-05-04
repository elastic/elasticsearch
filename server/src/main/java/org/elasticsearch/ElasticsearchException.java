/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystemLoopException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;

/**
 * A base class for all elasticsearch exceptions.
 */
public class ElasticsearchException extends RuntimeException implements ToXContentFragment, Writeable {

    private static final Version UNKNOWN_VERSION_ADDED = Version.fromId(0);

    /**
     * Passed in the {@link Params} of {@link #generateThrowableXContent(XContentBuilder, Params, Throwable)}
     * to control if the {@code caused_by} element should render. Unlike most parameters to {@code toXContent} methods this parameter is
     * internal only and not available as a URL parameter.
     */
    private static final String REST_EXCEPTION_SKIP_CAUSE = "rest.exception.cause.skip";
    /**
     * Passed in the {@link Params} of {@link #generateThrowableXContent(XContentBuilder, Params, Throwable)}
     * to control if the {@code stack_trace} element should render. Unlike most parameters to {@code toXContent} methods this parameter is
     * internal only and not available as a URL parameter. Use the {@code error_trace} parameter instead.
     */
    public static final String REST_EXCEPTION_SKIP_STACK_TRACE = "rest.exception.stacktrace.skip";
    public static final boolean REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT = true;
    private static final boolean REST_EXCEPTION_SKIP_CAUSE_DEFAULT = false;
    private static final String INDEX_METADATA_KEY = "es.index";
    private static final String INDEX_METADATA_KEY_UUID = "es.index_uuid";
    private static final String SHARD_METADATA_KEY = "es.shard";
    private static final String RESOURCE_METADATA_TYPE_KEY = "es.resource.type";
    private static final String RESOURCE_METADATA_ID_KEY = "es.resource.id";

    private static final String TYPE = "type";
    private static final String REASON = "reason";
    private static final String CAUSED_BY = "caused_by";
    private static final ParseField SUPPRESSED = new ParseField("suppressed");
    public static final String STACK_TRACE = "stack_trace";
    private static final String HEADER = "header";
    private static final String ERROR = "error";
    private static final String ROOT_CAUSE = "root_cause";

    private static final Map<Integer, CheckedFunction<StreamInput, ? extends ElasticsearchException, IOException>> ID_TO_SUPPLIER;
    private static final Map<Class<? extends ElasticsearchException>, ElasticsearchExceptionHandle> CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE;
    private final Map<String, List<String>> metadata = new HashMap<>();
    private final Map<String, List<String>> headers = new HashMap<>();

    /**
     * Construct a <code>ElasticsearchException</code> with the specified cause exception.
     */
    public ElasticsearchException(Throwable cause) {
        super(cause);
    }

    /**
     * Construct a <code>ElasticsearchException</code> with the specified detail message.
     *
     * The message can be parameterized using <code>{}</code> as placeholders for the given
     * arguments
     *
     * @param msg  the detail message
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
        super(in.readOptionalString(), in.readException());
        readStackTrace(this, in);
        headers.putAll(in.readMapOfLists(StreamInput::readString, StreamInput::readString));
        metadata.putAll(in.readMapOfLists(StreamInput::readString, StreamInput::readString));
    }

    /**
     * Adds a new piece of metadata with the given key.
     * If the provided key is already present, the corresponding metadata will be replaced
     */
    public void addMetadata(String key, String... values) {
        addMetadata(key, Arrays.asList(values));
    }

    /**
     * Adds a new piece of metadata with the given key.
     * If the provided key is already present, the corresponding metadata will be replaced
     */
    public void addMetadata(String key, List<String> values) {
        // we need to enforce this otherwise bw comp doesn't work properly, as "es." was the previous criteria to split headers in two sets
        if (key.startsWith("es.") == false) {
            throw new IllegalArgumentException("exception metadata must start with [es.], found [" + key + "] instead");
        }
        this.metadata.put(key, values);
    }

    /**
     * Returns a set of all metadata keys on this exception
     */
    public Set<String> getMetadataKeys() {
        return metadata.keySet();
    }

    /**
     * Returns the list of metadata values for the given key or {@code null} if no metadata for the
     * given key exists.
     */
    public List<String> getMetadata(String key) {
        return metadata.get(key);
    }

    protected Map<String, List<String>> getMetadata() {
        return metadata;
    }

    /**
     * Adds a new header with the given key.
     * This method will replace existing header if a header with the same key already exists
     */
    public void addHeader(String key, List<String> value) {
        // we need to enforce this otherwise bw comp doesn't work properly, as "es." was the previous criteria to split headers in two sets
        if (key.startsWith("es.")) {
            throw new IllegalArgumentException("exception headers must not start with [es.], found [" + key + "] instead");
        }
        this.headers.put(key, value);
    }

    /**
     * Adds a new header with the given key.
     * This method will replace existing header if a header with the same key already exists
     */
    public void addHeader(String key, String... value) {
        addHeader(key, Arrays.asList(value));
    }

    /**
     * Returns a set of all header keys on this exception
     */
    public Set<String> getHeaderKeys() {
        return headers.keySet();
    }

    /**
     * Returns the list of header values for the given key or {@code null} if no header for the
     * given key exists.
     */
    public List<String> getHeader(String key) {
        return headers.get(key);
    }

    protected Map<String, List<String>> getHeaders() {
        return headers;
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
     * @see ExceptionsHelper#unwrapCause(Throwable)
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

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        writeTo(out, createNestingFunction(0, () -> {}));
    }

    private static Writer<Throwable> createNestingFunction(int thisLevel, Runnable nestedExceptionLimitCallback) {
        int nextLevel = thisLevel + 1;
        return (o, t) -> {
            writeException(t.getCause(), o, nextLevel, nestedExceptionLimitCallback);
            writeStackTraces(t, o, (no, nt) -> writeException(nt, no, nextLevel, nestedExceptionLimitCallback));
        };
    }

    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        out.writeOptionalString(this.getMessage());
        nestedExceptionsWriter.write(out, this);
        out.writeMapOfLists(headers, StreamOutput::writeString, StreamOutput::writeString);
        out.writeMapOfLists(metadata, StreamOutput::writeString, StreamOutput::writeString);
    }

    public static ElasticsearchException readException(StreamInput input, int id) throws IOException {
        CheckedFunction<StreamInput, ? extends ElasticsearchException, IOException> elasticsearchException = ID_TO_SUPPLIER.get(id);
        if (elasticsearchException == null) {
            if (id == 127 && input.getVersion().before(Version.V_7_5_0)) {
                // was SearchContextException
                return new SearchException(input);
            }
            throw new IllegalStateException("unknown exception for id: " + id);
        }
        return elasticsearchException.apply(input);
    }

    /**
     * Returns <code>true</code> iff the given class is a registered for an exception to be read.
     */
    public static boolean isRegistered(Class<? extends Throwable> exception, Version version) {
        ElasticsearchExceptionHandle elasticsearchExceptionHandle = CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE.get(exception);
        if (elasticsearchExceptionHandle != null) {
            return version.onOrAfter(elasticsearchExceptionHandle.versionAdded);
        }
        return false;
    }

    static Set<Class<? extends ElasticsearchException>> getRegisteredKeys() { // for testing
        return CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE.keySet();
    }

    /**
     * Returns the serialization id the given exception.
     */
    public static int getId(Class<? extends ElasticsearchException> exception) {
        return CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE.get(exception).id;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Throwable ex = ExceptionsHelper.unwrapCause(this);
        if (ex != this) {
            generateThrowableXContent(builder, params, this);
        } else {
            innerToXContent(builder, params, this, getExceptionName(), getMessage(), headers, metadata, getCause());
        }
        return builder;
    }

    protected static void innerToXContent(
        XContentBuilder builder,
        Params params,
        Throwable throwable,
        String type,
        String message,
        Map<String, List<String>> headers,
        Map<String, List<String>> metadata,
        Throwable cause
    ) throws IOException {
        builder.field(TYPE, type);
        builder.field(REASON, message);

        for (Map.Entry<String, List<String>> entry : metadata.entrySet()) {
            headerToXContent(builder, entry.getKey().substring("es.".length()), entry.getValue());
        }

        if (throwable instanceof ElasticsearchException) {
            ElasticsearchException exception = (ElasticsearchException) throwable;
            exception.metadataToXContent(builder, params);
        }

        if (params.paramAsBoolean(REST_EXCEPTION_SKIP_CAUSE, REST_EXCEPTION_SKIP_CAUSE_DEFAULT) == false) {
            if (cause != null) {
                builder.field(CAUSED_BY);
                builder.startObject();
                generateThrowableXContent(builder, params, cause);
                builder.endObject();
            }
        }

        if (headers.isEmpty() == false) {
            builder.startObject(HEADER);
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                headerToXContent(builder, entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        if (params.paramAsBoolean(REST_EXCEPTION_SKIP_STACK_TRACE, REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT) == false) {
            builder.field(STACK_TRACE, ExceptionsHelper.stackTrace(throwable));
        }

        Throwable[] allSuppressed = throwable.getSuppressed();
        if (allSuppressed.length > 0) {
            builder.startArray(SUPPRESSED.getPreferredName());
            for (Throwable suppressed : allSuppressed) {
                builder.startObject();
                generateThrowableXContent(builder, params, suppressed);
                builder.endObject();
            }
            builder.endArray();
        }
    }

    private static void headerToXContent(XContentBuilder builder, String key, List<String> values) throws IOException {
        if (values != null && values.isEmpty() == false) {
            if (values.size() == 1) {
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
     * Renders additional per exception information into the XContent
     */
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {}

    /**
     * Generate a {@link ElasticsearchException} from a {@link XContentParser}. This does not
     * return the original exception type (ie NodeClosedException for example) but just wraps
     * the type, the reason and the cause of the exception. It also recursively parses the
     * tree structure of the cause, returning it as a tree structure of {@link ElasticsearchException}
     * instances.
     */
    public static ElasticsearchException fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        return innerFromXContent(parser, false);
    }

    public static ElasticsearchException innerFromXContent(XContentParser parser, boolean parseRootCauses) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String type = null, reason = null, stack = null;
        ElasticsearchException cause = null;
        Map<String, List<String>> metadata = new HashMap<>();
        Map<String, List<String>> headers = new HashMap<>();
        List<ElasticsearchException> rootCauses = new ArrayList<>();
        List<ElasticsearchException> suppressed = new ArrayList<>();

        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            String currentFieldName = parser.currentName();
            token = parser.nextToken();

            if (token.isValue()) {
                if (TYPE.equals(currentFieldName)) {
                    type = parser.text();
                } else if (REASON.equals(currentFieldName)) {
                    reason = parser.text();
                } else if (STACK_TRACE.equals(currentFieldName)) {
                    stack = parser.text();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    metadata.put(currentFieldName, Collections.singletonList(parser.text()));
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (CAUSED_BY.equals(currentFieldName)) {
                    cause = fromXContent(parser);
                } else if (HEADER.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else {
                            List<String> values = headers.getOrDefault(currentFieldName, new ArrayList<>());
                            if (token == XContentParser.Token.VALUE_STRING) {
                                values.add(parser.text());
                            } else if (token == XContentParser.Token.START_ARRAY) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    if (token == XContentParser.Token.VALUE_STRING) {
                                        values.add(parser.text());
                                    } else {
                                        parser.skipChildren();
                                    }
                                }
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                parser.skipChildren();
                            }
                            headers.put(currentFieldName, values);
                        }
                    }
                } else {
                    // Any additional metadata object added by the metadataToXContent method is ignored
                    // and skipped, so that the parser does not fail on unknown fields. The parser only
                    // support metadata key-pairs and metadata arrays of values.
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseRootCauses && ROOT_CAUSE.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        rootCauses.add(fromXContent(parser));
                    }
                } else if (SUPPRESSED.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        suppressed.add(fromXContent(parser));
                    }
                } else {
                    // Parse the array and add each item to the corresponding list of metadata.
                    // Arrays of objects are not supported yet and just ignored and skipped.
                    List<String> values = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            values.add(parser.text());
                        } else {
                            parser.skipChildren();
                        }
                    }
                    if (values.size() > 0) {
                        if (metadata.containsKey(currentFieldName)) {
                            values.addAll(metadata.get(currentFieldName));
                        }
                        metadata.put(currentFieldName, values);
                    }
                }
            }
        }

        ElasticsearchException e = new ElasticsearchException(buildMessage(type, reason, stack), cause);
        for (Map.Entry<String, List<String>> entry : metadata.entrySet()) {
            // subclasses can print out additional metadata through the metadataToXContent method. Simple key-value pairs will be
            // parsed back and become part of this metadata set, while objects and arrays are not supported when parsing back.
            // Those key-value pairs become part of the metadata set and inherit the "es." prefix as that is currently required
            // by addMetadata. The prefix will get stripped out when printing metadata out so it will be effectively invisible.
            // TODO move subclasses that print out simple metadata to using addMetadata directly and support also numbers and booleans.
            // TODO rename metadataToXContent and have only SearchPhaseExecutionException use it, which prints out complex objects
            e.addMetadata("es." + entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, List<String>> header : headers.entrySet()) {
            e.addHeader(header.getKey(), header.getValue());
        }

        // Adds root causes as suppressed exception. This way they are not lost
        // after parsing and can be retrieved using getSuppressed() method.
        for (ElasticsearchException rootCause : rootCauses) {
            e.addSuppressed(rootCause);
        }
        for (ElasticsearchException s : suppressed) {
            e.addSuppressed(s);
        }
        return e;
    }

    /**
     * Static toXContent helper method that renders {@link org.elasticsearch.ElasticsearchException} or {@link Throwable} instances
     * as XContent, delegating the rendering to {@link #toXContent(XContentBuilder, Params)}
     * or {@link #innerToXContent(XContentBuilder, Params, Throwable, String, String, Map, Map, Throwable)}.
     *
     * This method is usually used when the {@link Throwable} is rendered as a part of another XContent object, and its result can
     * be parsed back using the {@link #fromXContent(XContentParser)} method.
     */
    public static void generateThrowableXContent(XContentBuilder builder, Params params, Throwable t) throws IOException {
        t = ExceptionsHelper.unwrapCause(t);

        if (t instanceof ElasticsearchException) {
            ((ElasticsearchException) t).toXContent(builder, params);
        } else {
            innerToXContent(builder, params, t, getExceptionName(t), t.getMessage(), emptyMap(), emptyMap(), t.getCause());
        }
    }

    /**
     * Render any exception as a xcontent, encapsulated within a field or object named "error". The level of details that are rendered
     * depends on the value of the "detailed" parameter: when it's false only a simple message based on the type and message of the
     * exception is rendered. When it's true all detail are provided including guesses root causes, cause and potentially stack
     * trace.
     *
     * This method is usually used when the {@link Exception} is rendered as a full XContent object, and its output can be parsed
     * by the {@link #failureFromXContent(XContentParser)} method.
     */
    public static void generateFailureXContent(XContentBuilder builder, Params params, @Nullable Exception e, boolean detailed)
        throws IOException {
        // No exception to render as an error
        if (e == null) {
            builder.field(ERROR, "unknown");
            return;
        }

        // Render the exception with a simple message
        if (detailed == false) {
            String message = "No ElasticsearchException found";
            Throwable t = e;
            for (int counter = 0; counter < 10 && t != null; counter++) {
                if (t instanceof ElasticsearchException) {
                    message = t.getClass().getSimpleName() + "[" + t.getMessage() + "]";
                    break;
                }
                t = t.getCause();
            }
            builder.field(ERROR, message);
            return;
        }

        // Render the exception with all details
        final ElasticsearchException[] rootCauses = ElasticsearchException.guessRootCauses(e);
        builder.startObject(ERROR);
        {
            builder.startArray(ROOT_CAUSE);
            for (ElasticsearchException rootCause : rootCauses) {
                builder.startObject();
                rootCause.toXContent(builder, new DelegatingMapParams(singletonMap(REST_EXCEPTION_SKIP_CAUSE, "true"), params));
                builder.endObject();
            }
            builder.endArray();
        }
        generateThrowableXContent(builder, params, e);
        builder.endObject();
    }

    /**
     * Parses the output of {@link #generateFailureXContent(XContentBuilder, Params, Exception, boolean)}
     */
    public static ElasticsearchException failureFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureFieldName(parser, token, ERROR);

        token = parser.nextToken();
        if (token.isValue()) {
            return new ElasticsearchException(buildMessage("exception", parser.text(), null));
        }

        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();

        // Root causes are parsed in the innerFromXContent() and are added as suppressed exceptions.
        return innerFromXContent(parser, true);
    }

    /**
     * Returns the root cause of this exception or multiple if different shards caused different exceptions
     */
    public ElasticsearchException[] guessRootCauses() {
        final Throwable cause = getCause();
        if (cause != null && cause instanceof ElasticsearchException) {
            return ((ElasticsearchException) cause).guessRootCauses();
        }
        return new ElasticsearchException[] { this };
    }

    /**
     * Returns the root cause of this exception or multiple if different shards caused different exceptions.
     * If the given exception is not an instance of {@link org.elasticsearch.ElasticsearchException} an empty array
     * is returned.
     */
    public static ElasticsearchException[] guessRootCauses(Throwable t) {
        Throwable ex = ExceptionsHelper.unwrapCause(t);
        if (ex instanceof ElasticsearchException) {
            // ElasticsearchException knows how to guess its own root cause
            return ((ElasticsearchException) ex).guessRootCauses();
        }
        if (ex instanceof XContentParseException) {
            /*
             * We'd like to unwrap parsing exceptions to the inner-most
             * parsing exception because that is generally the most interesting
             * exception to return to the user. If that exception is caused by
             * an ElasticsearchException we'd like to keep unwrapping because
             * ElasticserachExceptions tend to contain useful information for
             * the user.
             */
            Throwable cause = ex.getCause();
            if (cause != null) {
                if (cause instanceof XContentParseException || cause instanceof ElasticsearchException) {
                    return guessRootCauses(ex.getCause());
                }
            }
        }
        return new ElasticsearchException[] { new ElasticsearchException(ex.getMessage(), ex) {
            @Override
            protected String getExceptionName() {
                return getExceptionName(getCause());
            }
        } };
    }

    protected String getExceptionName() {
        return getExceptionName(this);
    }

    /**
     * Returns a underscore case name for the given exception. This method strips {@code Elasticsearch} prefixes from exception names.
     */
    public static String getExceptionName(Throwable ex) {
        String simpleName = ex.getClass().getSimpleName();
        if (simpleName.startsWith("Elasticsearch")) {
            simpleName = simpleName.substring("Elasticsearch".length());
        }
        // TODO: do we really need to make the exception name in underscore casing?
        return toUnderscoreCase(simpleName);
    }

    static String buildMessage(String type, String reason, String stack) {
        StringBuilder message = new StringBuilder("Elasticsearch exception [");
        message.append(TYPE).append('=').append(type).append(", ");
        message.append(REASON).append('=').append(reason);
        if (stack != null) {
            message.append(", ").append(STACK_TRACE).append('=').append(stack);
        }
        message.append(']');
        return message.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (metadata.containsKey(INDEX_METADATA_KEY)) {
            builder.append(getIndex());
            if (metadata.containsKey(SHARD_METADATA_KEY)) {
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
        throwable.setStackTrace(in.readArray(i -> {
            final String declaringClasss = i.readString();
            final String fileName = i.readOptionalString();
            final String methodName = i.readString();
            final int lineNumber = i.readVInt();
            return new StackTraceElement(declaringClasss, methodName, fileName, lineNumber);
        }, StackTraceElement[]::new));

        int numSuppressed = in.readVInt();
        for (int i = 0; i < numSuppressed; i++) {
            throwable.addSuppressed(in.readException());
        }
        return throwable;
    }

    /**
     * Serializes the given exceptions stacktrace elements as well as it's suppressed exceptions to the given output stream.
     */
    public static void writeStackTraces(Throwable throwable, StreamOutput out, Writer<Throwable> exceptionWriter) throws IOException {
        out.writeArray((o, v) -> {
            o.writeString(v.getClassName());
            o.writeOptionalString(v.getFileName());
            o.writeString(v.getMethodName());
            o.writeVInt(v.getLineNumber());
        }, throwable.getStackTrace());
        out.writeArray(exceptionWriter, throwable.getSuppressed());
    }

    /**
     * Writes the specified {@code throwable} to {@link StreamOutput} {@code output}.
     */
    public static void writeException(Throwable throwable, StreamOutput output) throws IOException {
        writeException(throwable, output, () -> {});
    }

    static void writeException(Throwable throwable, StreamOutput output, Runnable nestedExceptionLimitCallback) throws IOException {
        writeException(throwable, output, 0, nestedExceptionLimitCallback);
    }

    private static final int MAX_NESTED_EXCEPTION_LEVEL = 100;

    private static void writeException(Throwable throwable, StreamOutput output, int nestedLevel, Runnable nestedExceptionLimitCallback)
        throws IOException {
        if (nestedLevel > MAX_NESTED_EXCEPTION_LEVEL) {
            nestedExceptionLimitCallback.run();
            writeException(new IllegalStateException("too many nested exceptions"), output);
            return;
        }

        if (throwable == null) {
            output.writeBoolean(false);
            return;
        }

        output.writeBoolean(true);

        boolean writeCause = true;
        boolean writeMessage = true;
        if (throwable instanceof CorruptIndexException) {
            output.writeVInt(1);
            output.writeOptionalString(((CorruptIndexException) throwable).getOriginalMessage());
            output.writeOptionalString(((CorruptIndexException) throwable).getResourceDescription());
            writeMessage = false;
        } else if (throwable instanceof IndexFormatTooNewException) {
            output.writeVInt(2);
            output.writeOptionalString(((IndexFormatTooNewException) throwable).getResourceDescription());
            output.writeInt(((IndexFormatTooNewException) throwable).getVersion());
            output.writeInt(((IndexFormatTooNewException) throwable).getMinVersion());
            output.writeInt(((IndexFormatTooNewException) throwable).getMaxVersion());
            writeMessage = false;
            writeCause = false;
        } else if (throwable instanceof IndexFormatTooOldException) {
            output.writeVInt(3);
            IndexFormatTooOldException t = (IndexFormatTooOldException) throwable;
            output.writeOptionalString(t.getResourceDescription());
            if (t.getVersion() == null) {
                output.writeBoolean(false);
                output.writeOptionalString(t.getReason());
            } else {
                output.writeBoolean(true);
                output.writeInt(t.getVersion());
                output.writeInt(t.getMinVersion());
                output.writeInt(t.getMaxVersion());
            }
            writeMessage = false;
            writeCause = false;
        } else if (throwable instanceof NullPointerException) {
            output.writeVInt(4);
            writeCause = false;
        } else if (throwable instanceof NumberFormatException) {
            output.writeVInt(5);
            writeCause = false;
        } else if (throwable instanceof IllegalArgumentException) {
            output.writeVInt(6);
        } else if (throwable instanceof AlreadyClosedException) {
            output.writeVInt(7);
        } else if (throwable instanceof EOFException) {
            output.writeVInt(8);
            writeCause = false;
        } else if (throwable instanceof SecurityException) {
            output.writeVInt(9);
        } else if (throwable instanceof StringIndexOutOfBoundsException) {
            output.writeVInt(10);
            writeCause = false;
        } else if (throwable instanceof ArrayIndexOutOfBoundsException) {
            output.writeVInt(11);
            writeCause = false;
        } else if (throwable instanceof FileNotFoundException) {
            output.writeVInt(12);
            writeCause = false;
        } else if (throwable instanceof FileSystemException) {
            output.writeVInt(13);
            if (throwable instanceof NoSuchFileException) {
                output.writeVInt(0);
            } else if (throwable instanceof NotDirectoryException) {
                output.writeVInt(1);
            } else if (throwable instanceof DirectoryNotEmptyException) {
                output.writeVInt(2);
            } else if (throwable instanceof AtomicMoveNotSupportedException) {
                output.writeVInt(3);
            } else if (throwable instanceof FileAlreadyExistsException) {
                output.writeVInt(4);
            } else if (throwable instanceof AccessDeniedException) {
                output.writeVInt(5);
            } else if (throwable instanceof FileSystemLoopException) {
                output.writeVInt(6);
            } else {
                output.writeVInt(7);
            }
            output.writeOptionalString(((FileSystemException) throwable).getFile());
            output.writeOptionalString(((FileSystemException) throwable).getOtherFile());
            output.writeOptionalString(((FileSystemException) throwable).getReason());
            writeCause = false;
        } else if (throwable instanceof IllegalStateException) {
            output.writeVInt(14);
        } else if (throwable instanceof LockObtainFailedException) {
            output.writeVInt(15);
        } else if (throwable instanceof InterruptedException) {
            output.writeVInt(16);
            writeCause = false;
        } else if (throwable instanceof IOException) {
            output.writeVInt(17);
        } else if (throwable instanceof EsRejectedExecutionException) {
            output.writeVInt(18);
            output.writeBoolean(((EsRejectedExecutionException) throwable).isExecutorShutdown());
            writeCause = false;
        } else {
            ElasticsearchException ex;
            if (throwable instanceof ElasticsearchException && isRegistered(throwable.getClass(), output.getVersion())) {
                ex = (ElasticsearchException) throwable;
            } else {
                ex = new NotSerializableExceptionWrapper(throwable);
            }
            output.writeVInt(0);
            output.writeVInt(getId(ex.getClass()));
            ex.writeTo(output, createNestingFunction(nestedLevel, nestedExceptionLimitCallback));
            return;
        }

        if (writeMessage) {
            output.writeOptionalString(throwable.getMessage());
        }
        if (writeCause) {
            writeException(throwable.getCause(), output, nestedLevel + 1, nestedExceptionLimitCallback);
        }
        writeStackTraces(throwable, output, (o, t) -> writeException(t, o, nestedLevel + 1, nestedExceptionLimitCallback));
    }

    /**
     * Reads a {@code Throwable} from {@link StreamInput} {@code input}.
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public static <T extends Throwable> T readException(StreamInput input) throws IOException {
        if (input.readBoolean()) {
            int key = input.readVInt();
            switch (key) {
                case 0:
                    final int ord = input.readVInt();
                    return (T) ElasticsearchException.readException(input, ord);
                case 1:
                    String msg1 = input.readOptionalString();
                    String resource1 = input.readOptionalString();
                    return (T) readStackTrace(new CorruptIndexException(msg1, resource1, readException(input)), input);
                case 2:
                    String resource2 = input.readOptionalString();
                    int version2 = input.readInt();
                    int minVersion2 = input.readInt();
                    int maxVersion2 = input.readInt();
                    return (T) readStackTrace(new IndexFormatTooNewException(resource2, version2, minVersion2, maxVersion2), input);
                case 3:
                    String resource3 = input.readOptionalString();
                    if (input.readBoolean()) {
                        int version3 = input.readInt();
                        int minVersion3 = input.readInt();
                        int maxVersion3 = input.readInt();
                        return (T) readStackTrace(new IndexFormatTooOldException(resource3, version3, minVersion3, maxVersion3), input);
                    } else {
                        String version3 = input.readOptionalString();
                        return (T) readStackTrace(new IndexFormatTooOldException(resource3, version3), input);
                    }
                case 4:
                    return (T) readStackTrace(new NullPointerException(input.readOptionalString()), input);
                case 5:
                    return (T) readStackTrace(new NumberFormatException(input.readOptionalString()), input);
                case 6:
                    return (T) readStackTrace(new IllegalArgumentException(input.readOptionalString(), readException(input)), input);
                case 7:
                    return (T) readStackTrace(new AlreadyClosedException(input.readOptionalString(), readException(input)), input);
                case 8:
                    return (T) readStackTrace(new EOFException(input.readOptionalString()), input);
                case 9:
                    return (T) readStackTrace(new SecurityException(input.readOptionalString(), readException(input)), input);
                case 10:
                    return (T) readStackTrace(new StringIndexOutOfBoundsException(input.readOptionalString()), input);
                case 11:
                    return (T) readStackTrace(new ArrayIndexOutOfBoundsException(input.readOptionalString()), input);
                case 12:
                    return (T) readStackTrace(new FileNotFoundException(input.readOptionalString()), input);
                case 13:
                    final int subclass = input.readVInt();
                    final String file = input.readOptionalString();
                    final String other = input.readOptionalString();
                    final String reason = input.readOptionalString();
                    input.readOptionalString(); // skip the msg - it's composed from file, other and reason
                    final Exception exception;
                    switch (subclass) {
                        case 0:
                            exception = new NoSuchFileException(file, other, reason);
                            break;
                        case 1:
                            exception = new NotDirectoryException(file);
                            break;
                        case 2:
                            exception = new DirectoryNotEmptyException(file);
                            break;
                        case 3:
                            exception = new AtomicMoveNotSupportedException(file, other, reason);
                            break;
                        case 4:
                            exception = new FileAlreadyExistsException(file, other, reason);
                            break;
                        case 5:
                            exception = new AccessDeniedException(file, other, reason);
                            break;
                        case 6:
                            exception = new FileSystemLoopException(file);
                            break;
                        case 7:
                            exception = new FileSystemException(file, other, reason);
                            break;
                        default:
                            throw new IllegalStateException("unknown FileSystemException with index " + subclass);
                    }
                    return (T) readStackTrace(exception, input);
                case 14:
                    return (T) readStackTrace(new IllegalStateException(input.readOptionalString(), readException(input)), input);
                case 15:
                    return (T) readStackTrace(new LockObtainFailedException(input.readOptionalString(), readException(input)), input);
                case 16:
                    return (T) readStackTrace(new InterruptedException(input.readOptionalString()), input);
                case 17:
                    return (T) readStackTrace(new IOException(input.readOptionalString(), readException(input)), input);
                case 18:
                    final boolean isExecutorShutdown = input.readBoolean();
                    return (T) readStackTrace(new EsRejectedExecutionException(input.readOptionalString(), isExecutorShutdown), input);
                default:
                    throw new IOException("no such exception for id: " + key);
            }
        }
        return null;
    }

    /**
     * This is the list of Exceptions Elasticsearch can throw over the wire or save into a corruption marker. Each value in the enum is a
     * single exception tying the Class to an id for use of the encode side and the id back to a constructor for use on the decode side. As
     * such its ok if the exceptions to change names so long as their constructor can still read the exception. Each exception is listed
     * in id order below. If you want to remove an exception leave a tombstone comment and mark the id as null in
     * ExceptionSerializationTests.testIds.ids.
     */
    private enum ElasticsearchExceptionHandle {
        INDEX_SHARD_SNAPSHOT_FAILED_EXCEPTION(
            org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException.class,
            org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException::new,
            0,
            UNKNOWN_VERSION_ADDED
        ),
        DFS_PHASE_EXECUTION_EXCEPTION(
            org.elasticsearch.search.dfs.DfsPhaseExecutionException.class,
            org.elasticsearch.search.dfs.DfsPhaseExecutionException::new,
            1,
            UNKNOWN_VERSION_ADDED
        ),
        EXECUTION_CANCELLED_EXCEPTION(
            org.elasticsearch.common.util.CancellableThreads.ExecutionCancelledException.class,
            org.elasticsearch.common.util.CancellableThreads.ExecutionCancelledException::new,
            2,
            UNKNOWN_VERSION_ADDED
        ),
        MASTER_NOT_DISCOVERED_EXCEPTION(
            org.elasticsearch.discovery.MasterNotDiscoveredException.class,
            org.elasticsearch.discovery.MasterNotDiscoveredException::new,
            3,
            UNKNOWN_VERSION_ADDED
        ),
        ELASTICSEARCH_SECURITY_EXCEPTION(
            org.elasticsearch.ElasticsearchSecurityException.class,
            org.elasticsearch.ElasticsearchSecurityException::new,
            4,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_RESTORE_EXCEPTION(
            org.elasticsearch.index.snapshots.IndexShardRestoreException.class,
            org.elasticsearch.index.snapshots.IndexShardRestoreException::new,
            5,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_CLOSED_EXCEPTION(
            org.elasticsearch.indices.IndexClosedException.class,
            org.elasticsearch.indices.IndexClosedException::new,
            6,
            UNKNOWN_VERSION_ADDED
        ),
        BIND_HTTP_EXCEPTION(
            org.elasticsearch.http.BindHttpException.class,
            org.elasticsearch.http.BindHttpException::new,
            7,
            UNKNOWN_VERSION_ADDED
        ),
        REDUCE_SEARCH_PHASE_EXCEPTION(
            org.elasticsearch.action.search.ReduceSearchPhaseException.class,
            org.elasticsearch.action.search.ReduceSearchPhaseException::new,
            8,
            UNKNOWN_VERSION_ADDED
        ),
        NODE_CLOSED_EXCEPTION(
            org.elasticsearch.node.NodeClosedException.class,
            org.elasticsearch.node.NodeClosedException::new,
            9,
            UNKNOWN_VERSION_ADDED
        ),
        // 10 was for SnapshotFailedEngineException, never instantiated in 6.2.0+ and never thrown across clusters
        SHARD_NOT_FOUND_EXCEPTION(
            org.elasticsearch.index.shard.ShardNotFoundException.class,
            org.elasticsearch.index.shard.ShardNotFoundException::new,
            11,
            UNKNOWN_VERSION_ADDED
        ),
        CONNECT_TRANSPORT_EXCEPTION(
            org.elasticsearch.transport.ConnectTransportException.class,
            org.elasticsearch.transport.ConnectTransportException::new,
            12,
            UNKNOWN_VERSION_ADDED
        ),
        NOT_SERIALIZABLE_TRANSPORT_EXCEPTION(
            org.elasticsearch.transport.NotSerializableTransportException.class,
            org.elasticsearch.transport.NotSerializableTransportException::new,
            13,
            UNKNOWN_VERSION_ADDED
        ),
        RESPONSE_HANDLER_FAILURE_TRANSPORT_EXCEPTION(
            org.elasticsearch.transport.ResponseHandlerFailureTransportException.class,
            org.elasticsearch.transport.ResponseHandlerFailureTransportException::new,
            14,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_CREATION_EXCEPTION(
            org.elasticsearch.indices.IndexCreationException.class,
            org.elasticsearch.indices.IndexCreationException::new,
            15,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_NOT_FOUND_EXCEPTION(
            org.elasticsearch.index.IndexNotFoundException.class,
            org.elasticsearch.index.IndexNotFoundException::new,
            16,
            UNKNOWN_VERSION_ADDED
        ),
        ILLEGAL_SHARD_ROUTING_STATE_EXCEPTION(
            org.elasticsearch.cluster.routing.IllegalShardRoutingStateException.class,
            org.elasticsearch.cluster.routing.IllegalShardRoutingStateException::new,
            17,
            UNKNOWN_VERSION_ADDED
        ),
        BROADCAST_SHARD_OPERATION_FAILED_EXCEPTION(
            org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException.class,
            org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException::new,
            18,
            UNKNOWN_VERSION_ADDED
        ),
        RESOURCE_NOT_FOUND_EXCEPTION(
            org.elasticsearch.ResourceNotFoundException.class,
            org.elasticsearch.ResourceNotFoundException::new,
            19,
            UNKNOWN_VERSION_ADDED
        ),
        ACTION_TRANSPORT_EXCEPTION(
            org.elasticsearch.transport.ActionTransportException.class,
            org.elasticsearch.transport.ActionTransportException::new,
            20,
            UNKNOWN_VERSION_ADDED
        ),
        ELASTICSEARCH_GENERATION_EXCEPTION(
            org.elasticsearch.ElasticsearchGenerationException.class,
            org.elasticsearch.ElasticsearchGenerationException::new,
            21,
            UNKNOWN_VERSION_ADDED
        ),
        // 22 was CreateFailedEngineException
        INDEX_SHARD_STARTED_EXCEPTION(
            org.elasticsearch.index.shard.IndexShardStartedException.class,
            org.elasticsearch.index.shard.IndexShardStartedException::new,
            23,
            UNKNOWN_VERSION_ADDED
        ),
        SEARCH_CONTEXT_MISSING_EXCEPTION(
            org.elasticsearch.search.SearchContextMissingException.class,
            org.elasticsearch.search.SearchContextMissingException::new,
            24,
            UNKNOWN_VERSION_ADDED
        ),
        GENERAL_SCRIPT_EXCEPTION(
            org.elasticsearch.script.GeneralScriptException.class,
            org.elasticsearch.script.GeneralScriptException::new,
            25,
            UNKNOWN_VERSION_ADDED
        ),
        // 26 was BatchOperationException
        SNAPSHOT_CREATION_EXCEPTION(
            org.elasticsearch.snapshots.SnapshotCreationException.class,
            org.elasticsearch.snapshots.SnapshotCreationException::new,
            27,
            UNKNOWN_VERSION_ADDED
        ),
        // 28 was DeleteFailedEngineException, deprecated in 6.0, removed in 7.0
        DOCUMENT_MISSING_EXCEPTION(
            org.elasticsearch.index.engine.DocumentMissingException.class,
            org.elasticsearch.index.engine.DocumentMissingException::new,
            29,
            UNKNOWN_VERSION_ADDED
        ),
        SNAPSHOT_EXCEPTION(
            org.elasticsearch.snapshots.SnapshotException.class,
            org.elasticsearch.snapshots.SnapshotException::new,
            30,
            UNKNOWN_VERSION_ADDED
        ),
        INVALID_ALIAS_NAME_EXCEPTION(
            org.elasticsearch.indices.InvalidAliasNameException.class,
            org.elasticsearch.indices.InvalidAliasNameException::new,
            31,
            UNKNOWN_VERSION_ADDED
        ),
        INVALID_INDEX_NAME_EXCEPTION(
            org.elasticsearch.indices.InvalidIndexNameException.class,
            org.elasticsearch.indices.InvalidIndexNameException::new,
            32,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_PRIMARY_SHARD_NOT_ALLOCATED_EXCEPTION(
            org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException.class,
            org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException::new,
            33,
            UNKNOWN_VERSION_ADDED
        ),
        TRANSPORT_EXCEPTION(
            org.elasticsearch.transport.TransportException.class,
            org.elasticsearch.transport.TransportException::new,
            34,
            UNKNOWN_VERSION_ADDED
        ),
        ELASTICSEARCH_PARSE_EXCEPTION(
            org.elasticsearch.ElasticsearchParseException.class,
            org.elasticsearch.ElasticsearchParseException::new,
            35,
            UNKNOWN_VERSION_ADDED
        ),
        SEARCH_EXCEPTION(
            org.elasticsearch.search.SearchException.class,
            org.elasticsearch.search.SearchException::new,
            36,
            UNKNOWN_VERSION_ADDED
        ),
        MAPPER_EXCEPTION(
            org.elasticsearch.index.mapper.MapperException.class,
            org.elasticsearch.index.mapper.MapperException::new,
            37,
            UNKNOWN_VERSION_ADDED
        ),
        INVALID_TYPE_NAME_EXCEPTION(
            org.elasticsearch.indices.InvalidTypeNameException.class,
            org.elasticsearch.indices.InvalidTypeNameException::new,
            38,
            UNKNOWN_VERSION_ADDED
        ),
        SNAPSHOT_RESTORE_EXCEPTION(
            org.elasticsearch.snapshots.SnapshotRestoreException.class,
            org.elasticsearch.snapshots.SnapshotRestoreException::new,
            39,
            UNKNOWN_VERSION_ADDED
        ),
        PARSING_EXCEPTION(
            org.elasticsearch.common.ParsingException.class,
            org.elasticsearch.common.ParsingException::new,
            40,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_CLOSED_EXCEPTION(
            org.elasticsearch.index.shard.IndexShardClosedException.class,
            org.elasticsearch.index.shard.IndexShardClosedException::new,
            41,
            UNKNOWN_VERSION_ADDED
        ),
        RECOVER_FILES_RECOVERY_EXCEPTION(
            org.elasticsearch.indices.recovery.RecoverFilesRecoveryException.class,
            org.elasticsearch.indices.recovery.RecoverFilesRecoveryException::new,
            42,
            UNKNOWN_VERSION_ADDED
        ),
        TRUNCATED_TRANSLOG_EXCEPTION(
            org.elasticsearch.index.translog.TruncatedTranslogException.class,
            org.elasticsearch.index.translog.TruncatedTranslogException::new,
            43,
            UNKNOWN_VERSION_ADDED
        ),
        RECOVERY_FAILED_EXCEPTION(
            org.elasticsearch.indices.recovery.RecoveryFailedException.class,
            org.elasticsearch.indices.recovery.RecoveryFailedException::new,
            44,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_RELOCATED_EXCEPTION(
            org.elasticsearch.index.shard.IndexShardRelocatedException.class,
            org.elasticsearch.index.shard.IndexShardRelocatedException::new,
            45,
            UNKNOWN_VERSION_ADDED
        ),
        // 46 was for NodeShouldNotConnectException, never instantiated in 5.0+
        // 47 used to be for IndexTemplateAlreadyExistsException which was deprecated in 5.1 removed in 6.0
        TRANSLOG_CORRUPTED_EXCEPTION(
            org.elasticsearch.index.translog.TranslogCorruptedException.class,
            org.elasticsearch.index.translog.TranslogCorruptedException::new,
            48,
            UNKNOWN_VERSION_ADDED
        ),
        CLUSTER_BLOCK_EXCEPTION(
            org.elasticsearch.cluster.block.ClusterBlockException.class,
            org.elasticsearch.cluster.block.ClusterBlockException::new,
            49,
            UNKNOWN_VERSION_ADDED
        ),
        FETCH_PHASE_EXECUTION_EXCEPTION(
            org.elasticsearch.search.fetch.FetchPhaseExecutionException.class,
            org.elasticsearch.search.fetch.FetchPhaseExecutionException::new,
            50,
            UNKNOWN_VERSION_ADDED
        ),
        // 51 used to be for IndexShardAlreadyExistsException which was deprecated in 5.1 removed in 6.0
        VERSION_CONFLICT_ENGINE_EXCEPTION(
            org.elasticsearch.index.engine.VersionConflictEngineException.class,
            org.elasticsearch.index.engine.VersionConflictEngineException::new,
            52,
            UNKNOWN_VERSION_ADDED
        ),
        ENGINE_EXCEPTION(
            org.elasticsearch.index.engine.EngineException.class,
            org.elasticsearch.index.engine.EngineException::new,
            53,
            UNKNOWN_VERSION_ADDED
        ),
        // 54 was DocumentAlreadyExistsException, which is superseded by VersionConflictEngineException
        NO_SUCH_NODE_EXCEPTION(
            org.elasticsearch.action.NoSuchNodeException.class,
            org.elasticsearch.action.NoSuchNodeException::new,
            55,
            UNKNOWN_VERSION_ADDED
        ),
        SETTINGS_EXCEPTION(
            org.elasticsearch.common.settings.SettingsException.class,
            org.elasticsearch.common.settings.SettingsException::new,
            56,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_TEMPLATE_MISSING_EXCEPTION(
            org.elasticsearch.indices.IndexTemplateMissingException.class,
            org.elasticsearch.indices.IndexTemplateMissingException::new,
            57,
            UNKNOWN_VERSION_ADDED
        ),
        SEND_REQUEST_TRANSPORT_EXCEPTION(
            org.elasticsearch.transport.SendRequestTransportException.class,
            org.elasticsearch.transport.SendRequestTransportException::new,
            58,
            UNKNOWN_VERSION_ADDED
        ),
        // 59 used to be EsRejectedExecutionException
        // 60 used to be for EarlyTerminationException
        // 61 used to be for RoutingValidationException
        NOT_SERIALIZABLE_EXCEPTION_WRAPPER(
            org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper.class,
            org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper::new,
            62,
            UNKNOWN_VERSION_ADDED
        ),
        ALIAS_FILTER_PARSING_EXCEPTION(
            org.elasticsearch.indices.AliasFilterParsingException.class,
            org.elasticsearch.indices.AliasFilterParsingException::new,
            63,
            UNKNOWN_VERSION_ADDED
        ),
        // 64 was DeleteByQueryFailedEngineException, which was removed in 5.0
        // 65 was for GatewayException, never instantiated in 5.0+
        INDEX_SHARD_NOT_RECOVERING_EXCEPTION(
            org.elasticsearch.index.shard.IndexShardNotRecoveringException.class,
            org.elasticsearch.index.shard.IndexShardNotRecoveringException::new,
            66,
            UNKNOWN_VERSION_ADDED
        ),
        HTTP_EXCEPTION(org.elasticsearch.http.HttpException.class, org.elasticsearch.http.HttpException::new, 67, UNKNOWN_VERSION_ADDED),
        ELASTICSEARCH_EXCEPTION(
            org.elasticsearch.ElasticsearchException.class,
            org.elasticsearch.ElasticsearchException::new,
            68,
            UNKNOWN_VERSION_ADDED
        ),
        SNAPSHOT_MISSING_EXCEPTION(
            org.elasticsearch.snapshots.SnapshotMissingException.class,
            org.elasticsearch.snapshots.SnapshotMissingException::new,
            69,
            UNKNOWN_VERSION_ADDED
        ),
        PRIMARY_MISSING_ACTION_EXCEPTION(
            org.elasticsearch.action.PrimaryMissingActionException.class,
            org.elasticsearch.action.PrimaryMissingActionException::new,
            70,
            UNKNOWN_VERSION_ADDED
        ),
        FAILED_NODE_EXCEPTION(
            org.elasticsearch.action.FailedNodeException.class,
            org.elasticsearch.action.FailedNodeException::new,
            71,
            UNKNOWN_VERSION_ADDED
        ),
        SEARCH_PARSE_EXCEPTION(
            org.elasticsearch.search.SearchParseException.class,
            org.elasticsearch.search.SearchParseException::new,
            72,
            UNKNOWN_VERSION_ADDED
        ),
        CONCURRENT_SNAPSHOT_EXECUTION_EXCEPTION(
            org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException.class,
            org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException::new,
            73,
            UNKNOWN_VERSION_ADDED
        ),
        BLOB_STORE_EXCEPTION(
            org.elasticsearch.common.blobstore.BlobStoreException.class,
            org.elasticsearch.common.blobstore.BlobStoreException::new,
            74,
            UNKNOWN_VERSION_ADDED
        ),
        INCOMPATIBLE_CLUSTER_STATE_VERSION_EXCEPTION(
            org.elasticsearch.cluster.IncompatibleClusterStateVersionException.class,
            org.elasticsearch.cluster.IncompatibleClusterStateVersionException::new,
            75,
            UNKNOWN_VERSION_ADDED
        ),
        RECOVERY_ENGINE_EXCEPTION(
            org.elasticsearch.index.engine.RecoveryEngineException.class,
            org.elasticsearch.index.engine.RecoveryEngineException::new,
            76,
            UNKNOWN_VERSION_ADDED
        ),
        UNCATEGORIZED_EXECUTION_EXCEPTION(
            org.elasticsearch.common.util.concurrent.UncategorizedExecutionException.class,
            org.elasticsearch.common.util.concurrent.UncategorizedExecutionException::new,
            77,
            UNKNOWN_VERSION_ADDED
        ),
        TIMESTAMP_PARSING_EXCEPTION(
            org.elasticsearch.action.TimestampParsingException.class,
            org.elasticsearch.action.TimestampParsingException::new,
            78,
            UNKNOWN_VERSION_ADDED
        ),
        ROUTING_MISSING_EXCEPTION(
            org.elasticsearch.action.RoutingMissingException.class,
            org.elasticsearch.action.RoutingMissingException::new,
            79,
            UNKNOWN_VERSION_ADDED
        ),
        // 80 was IndexFailedEngineException, deprecated in 6.0, removed in 7.0
        INDEX_SHARD_RESTORE_FAILED_EXCEPTION(
            org.elasticsearch.index.snapshots.IndexShardRestoreFailedException.class,
            org.elasticsearch.index.snapshots.IndexShardRestoreFailedException::new,
            81,
            UNKNOWN_VERSION_ADDED
        ),
        REPOSITORY_EXCEPTION(
            org.elasticsearch.repositories.RepositoryException.class,
            org.elasticsearch.repositories.RepositoryException::new,
            82,
            UNKNOWN_VERSION_ADDED
        ),
        RECEIVE_TIMEOUT_TRANSPORT_EXCEPTION(
            org.elasticsearch.transport.ReceiveTimeoutTransportException.class,
            org.elasticsearch.transport.ReceiveTimeoutTransportException::new,
            83,
            UNKNOWN_VERSION_ADDED
        ),
        NODE_DISCONNECTED_EXCEPTION(
            org.elasticsearch.transport.NodeDisconnectedException.class,
            org.elasticsearch.transport.NodeDisconnectedException::new,
            84,
            UNKNOWN_VERSION_ADDED
        ),
        // 85 used to be for AlreadyExpiredException
        AGGREGATION_EXECUTION_EXCEPTION(
            org.elasticsearch.search.aggregations.AggregationExecutionException.class,
            org.elasticsearch.search.aggregations.AggregationExecutionException::new,
            86,
            UNKNOWN_VERSION_ADDED
        ),
        // 87 used to be for MergeMappingException
        INVALID_INDEX_TEMPLATE_EXCEPTION(
            org.elasticsearch.indices.InvalidIndexTemplateException.class,
            org.elasticsearch.indices.InvalidIndexTemplateException::new,
            88,
            UNKNOWN_VERSION_ADDED
        ),
        REFRESH_FAILED_ENGINE_EXCEPTION(
            org.elasticsearch.index.engine.RefreshFailedEngineException.class,
            org.elasticsearch.index.engine.RefreshFailedEngineException::new,
            90,
            UNKNOWN_VERSION_ADDED
        ),
        AGGREGATION_INITIALIZATION_EXCEPTION(
            org.elasticsearch.search.aggregations.AggregationInitializationException.class,
            org.elasticsearch.search.aggregations.AggregationInitializationException::new,
            91,
            UNKNOWN_VERSION_ADDED
        ),
        DELAY_RECOVERY_EXCEPTION(
            org.elasticsearch.indices.recovery.DelayRecoveryException.class,
            org.elasticsearch.indices.recovery.DelayRecoveryException::new,
            92,
            UNKNOWN_VERSION_ADDED
        ),
        // 93 used to be for IndexWarmerMissingException
        NO_NODE_AVAILABLE_EXCEPTION(
            org.elasticsearch.client.transport.NoNodeAvailableException.class,
            org.elasticsearch.client.transport.NoNodeAvailableException::new,
            94,
            UNKNOWN_VERSION_ADDED
        ),
        INVALID_SNAPSHOT_NAME_EXCEPTION(
            org.elasticsearch.snapshots.InvalidSnapshotNameException.class,
            org.elasticsearch.snapshots.InvalidSnapshotNameException::new,
            96,
            UNKNOWN_VERSION_ADDED
        ),
        ILLEGAL_INDEX_SHARD_STATE_EXCEPTION(
            org.elasticsearch.index.shard.IllegalIndexShardStateException.class,
            org.elasticsearch.index.shard.IllegalIndexShardStateException::new,
            97,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_SNAPSHOT_EXCEPTION(
            org.elasticsearch.index.snapshots.IndexShardSnapshotException.class,
            org.elasticsearch.index.snapshots.IndexShardSnapshotException::new,
            98,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_NOT_STARTED_EXCEPTION(
            org.elasticsearch.index.shard.IndexShardNotStartedException.class,
            org.elasticsearch.index.shard.IndexShardNotStartedException::new,
            99,
            UNKNOWN_VERSION_ADDED
        ),
        SEARCH_PHASE_EXECUTION_EXCEPTION(
            org.elasticsearch.action.search.SearchPhaseExecutionException.class,
            org.elasticsearch.action.search.SearchPhaseExecutionException::new,
            100,
            UNKNOWN_VERSION_ADDED
        ),
        ACTION_NOT_FOUND_TRANSPORT_EXCEPTION(
            org.elasticsearch.transport.ActionNotFoundTransportException.class,
            org.elasticsearch.transport.ActionNotFoundTransportException::new,
            101,
            UNKNOWN_VERSION_ADDED
        ),
        TRANSPORT_SERIALIZATION_EXCEPTION(
            org.elasticsearch.transport.TransportSerializationException.class,
            org.elasticsearch.transport.TransportSerializationException::new,
            102,
            UNKNOWN_VERSION_ADDED
        ),
        REMOTE_TRANSPORT_EXCEPTION(
            org.elasticsearch.transport.RemoteTransportException.class,
            org.elasticsearch.transport.RemoteTransportException::new,
            103,
            UNKNOWN_VERSION_ADDED
        ),
        ENGINE_CREATION_FAILURE_EXCEPTION(
            org.elasticsearch.index.engine.EngineCreationFailureException.class,
            org.elasticsearch.index.engine.EngineCreationFailureException::new,
            104,
            UNKNOWN_VERSION_ADDED
        ),
        ROUTING_EXCEPTION(
            org.elasticsearch.cluster.routing.RoutingException.class,
            org.elasticsearch.cluster.routing.RoutingException::new,
            105,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_RECOVERY_EXCEPTION(
            org.elasticsearch.index.shard.IndexShardRecoveryException.class,
            org.elasticsearch.index.shard.IndexShardRecoveryException::new,
            106,
            UNKNOWN_VERSION_ADDED
        ),
        REPOSITORY_MISSING_EXCEPTION(
            org.elasticsearch.repositories.RepositoryMissingException.class,
            org.elasticsearch.repositories.RepositoryMissingException::new,
            107,
            UNKNOWN_VERSION_ADDED
        ),
        DOCUMENT_SOURCE_MISSING_EXCEPTION(
            org.elasticsearch.index.engine.DocumentSourceMissingException.class,
            org.elasticsearch.index.engine.DocumentSourceMissingException::new,
            109,
            UNKNOWN_VERSION_ADDED
        ),
        // 110 used to be FlushNotAllowedEngineException
        NO_CLASS_SETTINGS_EXCEPTION(
            org.elasticsearch.common.settings.NoClassSettingsException.class,
            org.elasticsearch.common.settings.NoClassSettingsException::new,
            111,
            UNKNOWN_VERSION_ADDED
        ),
        BIND_TRANSPORT_EXCEPTION(
            org.elasticsearch.transport.BindTransportException.class,
            org.elasticsearch.transport.BindTransportException::new,
            112,
            UNKNOWN_VERSION_ADDED
        ),
        ALIASES_NOT_FOUND_EXCEPTION(
            org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException.class,
            org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException::new,
            113,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_RECOVERING_EXCEPTION(
            org.elasticsearch.index.shard.IndexShardRecoveringException.class,
            org.elasticsearch.index.shard.IndexShardRecoveringException::new,
            114,
            UNKNOWN_VERSION_ADDED
        ),
        TRANSLOG_EXCEPTION(
            org.elasticsearch.index.translog.TranslogException.class,
            org.elasticsearch.index.translog.TranslogException::new,
            115,
            UNKNOWN_VERSION_ADDED
        ),
        PROCESS_CLUSTER_EVENT_TIMEOUT_EXCEPTION(
            org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException.class,
            org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException::new,
            116,
            UNKNOWN_VERSION_ADDED
        ),
        RETRY_ON_PRIMARY_EXCEPTION(
            ReplicationOperation.RetryOnPrimaryException.class,
            ReplicationOperation.RetryOnPrimaryException::new,
            117,
            UNKNOWN_VERSION_ADDED
        ),
        ELASTICSEARCH_TIMEOUT_EXCEPTION(
            org.elasticsearch.ElasticsearchTimeoutException.class,
            org.elasticsearch.ElasticsearchTimeoutException::new,
            118,
            UNKNOWN_VERSION_ADDED
        ),
        QUERY_PHASE_EXECUTION_EXCEPTION(
            org.elasticsearch.search.query.QueryPhaseExecutionException.class,
            org.elasticsearch.search.query.QueryPhaseExecutionException::new,
            119,
            UNKNOWN_VERSION_ADDED
        ),
        REPOSITORY_VERIFICATION_EXCEPTION(
            org.elasticsearch.repositories.RepositoryVerificationException.class,
            org.elasticsearch.repositories.RepositoryVerificationException::new,
            120,
            UNKNOWN_VERSION_ADDED
        ),
        INVALID_AGGREGATION_PATH_EXCEPTION(
            org.elasticsearch.search.aggregations.InvalidAggregationPathException.class,
            org.elasticsearch.search.aggregations.InvalidAggregationPathException::new,
            121,
            UNKNOWN_VERSION_ADDED
        ),
        // 123 used to be IndexAlreadyExistsException and was renamed
        RESOURCE_ALREADY_EXISTS_EXCEPTION(
            ResourceAlreadyExistsException.class,
            ResourceAlreadyExistsException::new,
            123,
            UNKNOWN_VERSION_ADDED
        ),
        // 124 used to be Script.ScriptParseException
        HTTP_REQUEST_ON_TRANSPORT_EXCEPTION(
            TcpTransport.HttpRequestOnTransportException.class,
            TcpTransport.HttpRequestOnTransportException::new,
            125,
            UNKNOWN_VERSION_ADDED
        ),
        MAPPER_PARSING_EXCEPTION(
            org.elasticsearch.index.mapper.MapperParsingException.class,
            org.elasticsearch.index.mapper.MapperParsingException::new,
            126,
            UNKNOWN_VERSION_ADDED
        ),
        // 127 used to be org.elasticsearch.search.SearchContextException
        SEARCH_SOURCE_BUILDER_EXCEPTION(
            org.elasticsearch.search.builder.SearchSourceBuilderException.class,
            org.elasticsearch.search.builder.SearchSourceBuilderException::new,
            128,
            UNKNOWN_VERSION_ADDED
        ),
        // 129 was EngineClosedException
        NO_SHARD_AVAILABLE_ACTION_EXCEPTION(
            org.elasticsearch.action.NoShardAvailableActionException.class,
            org.elasticsearch.action.NoShardAvailableActionException::new,
            130,
            UNKNOWN_VERSION_ADDED
        ),
        UNAVAILABLE_SHARDS_EXCEPTION(
            org.elasticsearch.action.UnavailableShardsException.class,
            org.elasticsearch.action.UnavailableShardsException::new,
            131,
            UNKNOWN_VERSION_ADDED
        ),
        FLUSH_FAILED_ENGINE_EXCEPTION(
            org.elasticsearch.index.engine.FlushFailedEngineException.class,
            org.elasticsearch.index.engine.FlushFailedEngineException::new,
            132,
            UNKNOWN_VERSION_ADDED
        ),
        CIRCUIT_BREAKING_EXCEPTION(
            org.elasticsearch.common.breaker.CircuitBreakingException.class,
            org.elasticsearch.common.breaker.CircuitBreakingException::new,
            133,
            UNKNOWN_VERSION_ADDED
        ),
        NODE_NOT_CONNECTED_EXCEPTION(
            org.elasticsearch.transport.NodeNotConnectedException.class,
            org.elasticsearch.transport.NodeNotConnectedException::new,
            134,
            UNKNOWN_VERSION_ADDED
        ),
        STRICT_DYNAMIC_MAPPING_EXCEPTION(
            org.elasticsearch.index.mapper.StrictDynamicMappingException.class,
            org.elasticsearch.index.mapper.StrictDynamicMappingException::new,
            135,
            UNKNOWN_VERSION_ADDED
        ),
        RETRY_ON_REPLICA_EXCEPTION(
            org.elasticsearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException.class,
            org.elasticsearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException::new,
            136,
            UNKNOWN_VERSION_ADDED
        ),
        TYPE_MISSING_EXCEPTION(
            org.elasticsearch.indices.TypeMissingException.class,
            org.elasticsearch.indices.TypeMissingException::new,
            137,
            UNKNOWN_VERSION_ADDED
        ),
        FAILED_TO_COMMIT_CLUSTER_STATE_EXCEPTION(
            org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException.class,
            org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException::new,
            140,
            UNKNOWN_VERSION_ADDED
        ),
        QUERY_SHARD_EXCEPTION(
            org.elasticsearch.index.query.QueryShardException.class,
            org.elasticsearch.index.query.QueryShardException::new,
            141,
            UNKNOWN_VERSION_ADDED
        ),
        NO_LONGER_PRIMARY_SHARD_EXCEPTION(
            ShardStateAction.NoLongerPrimaryShardException.class,
            ShardStateAction.NoLongerPrimaryShardException::new,
            142,
            UNKNOWN_VERSION_ADDED
        ),
        SCRIPT_EXCEPTION(
            org.elasticsearch.script.ScriptException.class,
            org.elasticsearch.script.ScriptException::new,
            143,
            UNKNOWN_VERSION_ADDED
        ),
        NOT_MASTER_EXCEPTION(
            org.elasticsearch.cluster.NotMasterException.class,
            org.elasticsearch.cluster.NotMasterException::new,
            144,
            UNKNOWN_VERSION_ADDED
        ),
        STATUS_EXCEPTION(
            org.elasticsearch.ElasticsearchStatusException.class,
            org.elasticsearch.ElasticsearchStatusException::new,
            145,
            UNKNOWN_VERSION_ADDED
        ),
        TASK_CANCELLED_EXCEPTION(
            org.elasticsearch.tasks.TaskCancelledException.class,
            org.elasticsearch.tasks.TaskCancelledException::new,
            146,
            UNKNOWN_VERSION_ADDED
        ),
        SHARD_LOCK_OBTAIN_FAILED_EXCEPTION(
            org.elasticsearch.env.ShardLockObtainFailedException.class,
            org.elasticsearch.env.ShardLockObtainFailedException::new,
            147,
            UNKNOWN_VERSION_ADDED
        ),
        // 148 was UnknownNamedObjectException
        TOO_MANY_BUCKETS_EXCEPTION(
            MultiBucketConsumerService.TooManyBucketsException.class,
            MultiBucketConsumerService.TooManyBucketsException::new,
            149,
            Version.V_6_2_0
        ),
        COORDINATION_STATE_REJECTED_EXCEPTION(
            org.elasticsearch.cluster.coordination.CoordinationStateRejectedException.class,
            org.elasticsearch.cluster.coordination.CoordinationStateRejectedException::new,
            150,
            Version.V_7_0_0
        ),
        SNAPSHOT_IN_PROGRESS_EXCEPTION(
            org.elasticsearch.snapshots.SnapshotInProgressException.class,
            org.elasticsearch.snapshots.SnapshotInProgressException::new,
            151,
            Version.V_6_7_0
        ),
        NO_SUCH_REMOTE_CLUSTER_EXCEPTION(
            org.elasticsearch.transport.NoSuchRemoteClusterException.class,
            org.elasticsearch.transport.NoSuchRemoteClusterException::new,
            152,
            Version.V_6_7_0
        ),
        RETENTION_LEASE_ALREADY_EXISTS_EXCEPTION(
            org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException.class,
            org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException::new,
            153,
            Version.V_6_7_0
        ),
        RETENTION_LEASE_NOT_FOUND_EXCEPTION(
            org.elasticsearch.index.seqno.RetentionLeaseNotFoundException.class,
            org.elasticsearch.index.seqno.RetentionLeaseNotFoundException::new,
            154,
            Version.V_6_7_0
        ),
        SHARD_NOT_IN_PRIMARY_MODE_EXCEPTION(
            org.elasticsearch.index.shard.ShardNotInPrimaryModeException.class,
            org.elasticsearch.index.shard.ShardNotInPrimaryModeException::new,
            155,
            Version.V_6_8_1
        ),
        RETENTION_LEASE_INVALID_RETAINING_SEQUENCE_NUMBER_EXCEPTION(
            org.elasticsearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException.class,
            org.elasticsearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException::new,
            156,
            Version.V_7_5_0
        ),
        INGEST_PROCESSOR_EXCEPTION(
            org.elasticsearch.ingest.IngestProcessorException.class,
            org.elasticsearch.ingest.IngestProcessorException::new,
            157,
            Version.V_7_5_0
        ),
        PEER_RECOVERY_NOT_FOUND_EXCEPTION(
            org.elasticsearch.indices.recovery.PeerRecoveryNotFound.class,
            org.elasticsearch.indices.recovery.PeerRecoveryNotFound::new,
            158,
            Version.V_7_9_0
        ),
        NODE_HEALTH_CHECK_FAILURE_EXCEPTION(
            org.elasticsearch.cluster.coordination.NodeHealthCheckFailureException.class,
            org.elasticsearch.cluster.coordination.NodeHealthCheckFailureException::new,
            159,
            Version.V_7_9_0
        ),
        NO_SEED_NODE_LEFT_EXCEPTION(
            org.elasticsearch.transport.NoSeedNodeLeftException.class,
            org.elasticsearch.transport.NoSeedNodeLeftException::new,
            160,
            Version.V_7_10_0
        ),
        VERSION_MISMATCH_EXCEPTION(
            org.elasticsearch.action.search.VersionMismatchException.class,
            org.elasticsearch.action.search.VersionMismatchException::new,
            161,
            Version.V_7_12_0
        ),
        HTTP_HEADERS_VALIDATION_EXCEPTION(
            org.elasticsearch.http.HttpHeadersValidationException.class,
            org.elasticsearch.http.HttpHeadersValidationException::new,
            162,
            Version.V_7_17_11
        );

        final Class<? extends ElasticsearchException> exceptionClass;
        final CheckedFunction<StreamInput, ? extends ElasticsearchException, IOException> constructor;
        final int id;
        final Version versionAdded;

        <E extends ElasticsearchException> ElasticsearchExceptionHandle(
            Class<E> exceptionClass,
            CheckedFunction<StreamInput, E, IOException> constructor,
            int id,
            Version versionAdded
        ) {
            // We need the exceptionClass because you can't dig it out of the constructor reliably.
            this.exceptionClass = exceptionClass;
            this.constructor = constructor;
            this.versionAdded = versionAdded;
            this.id = id;
        }
    }

    /**
     * Returns an array of all registered handle IDs. These are the IDs for every registered
     * exception.
     *
     * @return an array of all registered handle IDs
     */
    static int[] ids() {
        return Arrays.stream(ElasticsearchExceptionHandle.values()).mapToInt(h -> h.id).toArray();
    }

    /**
     * Returns an array of all registered pairs of handle IDs and exception classes. These pairs are
     * provided for every registered exception.
     *
     * @return an array of all registered pairs of handle IDs and exception classes
     */
    static Tuple<Integer, Class<? extends ElasticsearchException>>[] classes() {
        @SuppressWarnings("unchecked")
        final Tuple<Integer, Class<? extends ElasticsearchException>>[] ts = Arrays.stream(ElasticsearchExceptionHandle.values())
            .map(h -> Tuple.tuple(h.id, h.exceptionClass))
            .toArray(Tuple[]::new);
        return ts;
    }

    static {
        ID_TO_SUPPLIER = unmodifiableMap(
            Arrays.stream(ElasticsearchExceptionHandle.values()).collect(Collectors.toMap(e -> e.id, e -> e.constructor))
        );
        CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE = unmodifiableMap(
            Arrays.stream(ElasticsearchExceptionHandle.values()).collect(Collectors.toMap(e -> e.exceptionClass, e -> e))
        );
    }

    public Index getIndex() {
        List<String> index = getMetadata(INDEX_METADATA_KEY);
        if (index != null && index.isEmpty() == false) {
            List<String> index_uuid = getMetadata(INDEX_METADATA_KEY_UUID);
            return new Index(index.get(0), index_uuid.get(0));
        }

        return null;
    }

    public ShardId getShardId() {
        List<String> shard = getMetadata(SHARD_METADATA_KEY);
        if (shard != null && shard.isEmpty() == false) {
            return new ShardId(getIndex(), Integer.parseInt(shard.get(0)));
        }
        return null;
    }

    public void setIndex(Index index) {
        if (index != null) {
            addMetadata(INDEX_METADATA_KEY, index.getName());
            addMetadata(INDEX_METADATA_KEY_UUID, index.getUUID());
        }
    }

    public void setIndex(String index) {
        if (index != null) {
            setIndex(new Index(index, INDEX_UUID_NA_VALUE));
        }
    }

    public void setShard(ShardId shardId) {
        if (shardId != null) {
            setIndex(shardId.getIndex());
            addMetadata(SHARD_METADATA_KEY, Integer.toString(shardId.id()));
        }
    }

    public void setResources(String type, String... id) {
        assert type != null;
        addMetadata(RESOURCE_METADATA_ID_KEY, id);
        addMetadata(RESOURCE_METADATA_TYPE_KEY, type);
    }

    public List<String> getResourceId() {
        return getMetadata(RESOURCE_METADATA_ID_KEY);
    }

    public String getResourceType() {
        List<String> header = getMetadata(RESOURCE_METADATA_TYPE_KEY);
        if (header != null && header.isEmpty() == false) {
            assert header.size() == 1;
            return header.get(0);
        }
        return null;
    }

    // lower cases and adds underscores to transitions in a name
    private static String toUnderscoreCase(String value) {
        StringBuilder sb = new StringBuilder();
        boolean changed = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isUpperCase(c)) {
                if (changed == false) {
                    // copy it over here
                    for (int j = 0; j < i; j++) {
                        sb.append(value.charAt(j));
                    }
                    changed = true;
                    if (i == 0) {
                        sb.append(Character.toLowerCase(c));
                    } else {
                        sb.append('_');
                        sb.append(Character.toLowerCase(c));
                    }
                } else {
                    sb.append('_');
                    sb.append(Character.toLowerCase(c));
                }
            } else {
                if (changed) {
                    sb.append(c);
                }
            }
        }
        if (changed == false) {
            return value;
        }
        return sb.toString();
    }

}
