/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.index.VersionType.INTERNAL;

/**
 * Request to reindex some documents from one index to another. This implements CompositeIndicesRequest but in a misleading way. Rather than
 * returning all the subrequests that it will make it tries to return a representative set of subrequests. This is best-effort for a bunch
 * of reasons, not least of which that scripts are allowed to change the destination request in drastic ways, including changing the index
 * to which documents are written.
 */
public class ReindexRequest extends AbstractBulkIndexByScrollRequest<ReindexRequest> implements CompositeIndicesRequest, ToXContentObject {
    /**
     * Prototype for index requests.
     */
    private final IndexRequest destination;

    private RemoteInfo remoteInfo;

    public ReindexRequest() {
        this(new SearchRequest(), new IndexRequest(), true);
    }

    ReindexRequest(SearchRequest search, IndexRequest destination, boolean setDefaults) {
        super(search, setDefaults);
        this.destination = destination;
    }

    public ReindexRequest(StreamInput in) throws IOException {
        super(in);
        destination = new IndexRequest(in);
        remoteInfo = in.readOptionalWriteable(RemoteInfo::new);
    }

    @Override
    protected ReindexRequest self() {
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = super.validate();
        if (getSearchRequest().indices() == null || getSearchRequest().indices().length == 0) {
            e = addValidationError("use _all if you really want to copy from all existing indexes", e);
        }
        if (getSearchRequest().source().fetchSource() != null && getSearchRequest().source().fetchSource().fetchSource() == false) {
            e = addValidationError("_source:false is not supported in this context", e);
        }
        /*
         * Note that we don't call index's validator - it won't work because
         * we'll be filling in portions of it as we receive the docs. But we can
         * validate some things so we do that below.
         */
        if (destination.index() == null) {
            e = addValidationError("index must be specified", e);
            return e;
        }
        if (false == routingIsValid()) {
            e = addValidationError("routing must be unset, [keep], [discard] or [=<some new value>]", e);
        }
        if (destination.versionType() == INTERNAL) {
            if (destination.version() != Versions.MATCH_ANY && destination.version() != Versions.MATCH_DELETED) {
                e = addValidationError("unsupported version for internal versioning [" + destination.version() + ']', e);
            }
        }
        if (getRemoteInfo() != null) {
            if (getSearchRequest().source().query() != null) {
                e = addValidationError("reindex from remote sources should use RemoteInfo's query instead of source's query", e);
            }
            if (getSlices() == AbstractBulkByScrollRequest.AUTO_SLICES || getSlices() > 1) {
                e = addValidationError("reindex from remote sources doesn't support slices > 1 but was [" + getSlices() + "]", e);
            }
        }
        return e;
    }

    private boolean routingIsValid() {
        if (destination.routing() == null || destination.routing().startsWith("=")) {
            return true;
        }
        return switch (destination.routing()) {
            case "keep", "discard" -> true;
            default -> false;
        };
    }

    /**
     * Set the indices which will act as the source for the ReindexRequest
     */
    public ReindexRequest setSourceIndices(String... sourceIndices) {
        if (sourceIndices != null) {
            this.getSearchRequest().indices(sourceIndices);
        }
        return this;
    }

    /**
     * Sets the scroll size for setting how many documents are to be processed in one batch during reindex
     */
    public ReindexRequest setSourceBatchSize(int size) {
        this.getSearchRequest().source().size(size);
        return this;
    }

    /**
     * Set the query for selecting documents from the source indices
     */
    public ReindexRequest setSourceQuery(QueryBuilder queryBuilder) {
        if (queryBuilder != null) {
            this.getSearchRequest().source().query(queryBuilder);
        }
        return this;
    }

    /**
     * Add a sort against the given field name.
     *
     * @param name The name of the field to sort by
     * @param order The order in which to sort
     * @deprecated Specifying a sort field for reindex is deprecated. If using this in combination with maxDocs, consider using a
     * query filter instead.
     */
    @Deprecated
    public ReindexRequest addSortField(String name, SortOrder order) {
        this.getSearchRequest().source().sort(name, order);
        return this;
    }

    /**
     * Set the target index for the ReindexRequest
     */
    public ReindexRequest setDestIndex(String destIndex) {
        if (destIndex != null) {
            this.getDestination().index(destIndex);
        }
        return this;
    }

    /**
     * Set the routing to decide which shard the documents need to be routed to
     */
    public ReindexRequest setDestRouting(String routing) {
        this.getDestination().routing(routing);
        return this;
    }

    /**
     * Set the version type for the target index. A {@link VersionType#EXTERNAL} helps preserve the version
     * if the document already existed in the target index.
     */
    public ReindexRequest setDestVersionType(VersionType versionType) {
        this.getDestination().versionType(versionType);
        return this;
    }

    /**
     * Allows to set the ingest pipeline for the target index.
     */
    public void setDestPipeline(String pipelineName) {
        this.getDestination().setPipeline(pipelineName);
    }

    /**
     * Sets the optype on the destination index
     * @param opType must be one of {create, index}
     */
    public ReindexRequest setDestOpType(String opType) {
        this.getDestination().opType(opType);
        return this;
    }

    /**
     * Set the {@link RemoteInfo} if the source indices are in a remote cluster.
     */
    public ReindexRequest setRemoteInfo(RemoteInfo remoteInfo) {
        this.remoteInfo = remoteInfo;
        return this;
    }

    /**
     * Sets the require_alias request flag on the destination index
     */
    public ReindexRequest setRequireAlias(boolean requireAlias) {
        this.getDestination().setRequireAlias(requireAlias);
        return this;
    }

    /**
     * Gets the target for this reindex request in the for of an {@link IndexRequest}
     */
    public IndexRequest getDestination() {
        return destination;
    }

    /**
     * Get the {@link RemoteInfo} if it was set for this request.
     */
    public RemoteInfo getRemoteInfo() {
        return remoteInfo;
    }

    @Override
    public ReindexRequest forSlice(TaskId slicingTask, SearchRequest slice, int totalSlices) {
        ReindexRequest sliced = doForSlice(new ReindexRequest(slice, destination, false), slicingTask, totalSlices);
        sliced.setRemoteInfo(remoteInfo);
        return sliced;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        destination.writeTo(out);
        out.writeOptionalWriteable(remoteInfo);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("reindex from ");
        if (remoteInfo != null) {
            b.append('[').append(remoteInfo).append(']');
        }
        searchToString(b);
        b.append(" to [").append(destination.index()).append(']');
        return b.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            // build source
            builder.startObject("source");
            if (remoteInfo != null) {
                builder.field("remote", remoteInfo);
                builder.rawField("query", remoteInfo.getQuery().streamInput(), RemoteInfo.QUERY_CONTENT_TYPE.type());
            }
            builder.array("index", getSearchRequest().indices());
            getSearchRequest().source().innerToXContent(builder, params);
            builder.endObject();
        }
        {
            // build destination
            builder.startObject("dest");
            builder.field("index", getDestination().index());
            if (getDestination().routing() != null) {
                builder.field("routing", getDestination().routing());
            }
            builder.field("op_type", getDestination().opType().getLowercase());
            if (getDestination().getPipeline() != null) {
                builder.field("pipeline", getDestination().getPipeline());
            }
            builder.field("version_type", VersionType.toString(getDestination().versionType()));
            builder.endObject();
        }
        {
            // Other fields
            if (getMaxDocs() != -1) {
                builder.field("max_docs", getMaxDocs());
            }
            if (getScript() != null) {
                builder.field("script", getScript());
            }
            if (isAbortOnVersionConflict() == false) {
                builder.field("conflicts", "proceed");
            }
        }
        builder.endObject();
        return builder;
    }

    static final ObjectParser<ReindexRequest, Predicate<NodeFeature>> PARSER = new ObjectParser<>("reindex");

    static {
        ObjectParser.Parser<ReindexRequest, Predicate<NodeFeature>> sourceParser = (parser, request, context) -> {
            // Funky hack to work around Search not having a proper ObjectParser and us wanting to extract query if using remote.
            Map<String, Object> source = parser.map();
            String[] indices = extractStringArray(source, "index");
            if (indices != null) {
                request.getSearchRequest().indices(indices);
            }
            request.setRemoteInfo(buildRemoteInfo(source));
            XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
            builder.map(source);
            try (
                XContentParser innerParser = XContentHelper.createParserNotCompressed(
                    XContentParserConfiguration.EMPTY.withRegistry(parser.getXContentRegistry())
                        .withDeprecationHandler(parser.getDeprecationHandler()),
                    BytesReference.bytes(builder),
                    parser.contentType()
                )
            ) {
                request.getSearchRequest().source().parseXContent(innerParser, false, context);
            }
        };

        ObjectParser<IndexRequest, Void> destParser = new ObjectParser<>("dest");
        destParser.declareString(IndexRequest::index, new ParseField("index"));
        destParser.declareString(IndexRequest::routing, new ParseField("routing"));
        destParser.declareString(IndexRequest::opType, new ParseField("op_type"));
        destParser.declareString(IndexRequest::setPipeline, new ParseField("pipeline"));
        destParser.declareString((s, i) -> s.versionType(VersionType.fromString(i)), new ParseField("version_type"));

        PARSER.declareField(sourceParser, new ParseField("source"), ObjectParser.ValueType.OBJECT);
        PARSER.declareField(
            (p, v, c) -> destParser.parse(p, v.getDestination(), null),
            new ParseField("dest"),
            ObjectParser.ValueType.OBJECT
        );

        PARSER.declareInt(ReindexRequest::setMaxDocsValidateIdentical, new ParseField("max_docs"));
        PARSER.declareField((p, v, c) -> v.setScript(Script.parse(p)), new ParseField("script"), ObjectParser.ValueType.OBJECT);
        PARSER.declareString(ReindexRequest::setConflicts, new ParseField("conflicts"));
    }

    public static ReindexRequest fromXContent(XContentParser parser, Predicate<NodeFeature> clusterSupportsFeature) throws IOException {
        ReindexRequest reindexRequest = new ReindexRequest();
        PARSER.parse(parser, reindexRequest, clusterSupportsFeature);
        return reindexRequest;
    }

    /**
     * Yank a string array from a map. Emulates XContent's permissive String to
     * String array conversions and allow comma separated String.
     */
    private static String[] extractStringArray(Map<String, Object> source, String name) {
        Object value = source.remove(name);
        if (value == null) {
            return null;
        }
        if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> list = (List<String>) value;
            return list.toArray(new String[list.size()]);
        } else if (value instanceof String) {
            return Strings.splitStringByCommaToArray((String) value);
        } else {
            throw new IllegalArgumentException("Expected [" + name + "] to be a list or a string but was [" + value + ']');
        }
    }

    static RemoteInfo buildRemoteInfo(Map<String, Object> source) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> remote = (Map<String, Object>) source.remove("remote");
        if (remote == null) {
            return null;
        }
        String username = extractString(remote, "username");
        SecureString password = extractSecureString(remote, "password");
        String hostInRequest = requireNonNull(extractString(remote, "host"), "[host] must be specified to reindex from a remote cluster");
        URI uri;
        try {
            uri = new URI(hostInRequest);
            // URI has less stringent URL parsing than our code. We want to fail if all values are not provided.
            if (uri.getPort() == -1) {
                throw new URISyntaxException(hostInRequest, "The port was not defined in the [host]");
            }
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException(
                "[host] must be of the form [scheme]://[host]:[port](/[pathPrefix])? but was [" + hostInRequest + "]",
                ex
            );
        }

        String scheme = uri.getScheme();
        String host = uri.getHost();
        int port = uri.getPort();

        String pathPrefix = null;
        if (uri.getPath().isEmpty() == false) {
            pathPrefix = uri.getPath();
        }

        Map<String, String> headers = extractStringStringMap(remote, "headers");
        TimeValue socketTimeout = extractTimeValue(remote, "socket_timeout", RemoteInfo.DEFAULT_SOCKET_TIMEOUT);
        TimeValue connectTimeout = extractTimeValue(remote, "connect_timeout", RemoteInfo.DEFAULT_CONNECT_TIMEOUT);
        if (false == remote.isEmpty()) {
            throw new IllegalArgumentException(
                "Unsupported fields in [remote]: [" + Strings.collectionToCommaDelimitedString(remote.keySet()) + "]"
            );
        }
        return new RemoteInfo(
            scheme,
            host,
            port,
            pathPrefix,
            RemoteInfo.queryForRemote(source),
            username,
            password,
            headers,
            socketTimeout,
            connectTimeout
        );
    }

    private static String extractString(Map<String, Object> source, String name) {
        Object value = source.remove(name);
        if (value == null) {
            return null;
        }
        if (value instanceof String str) {
            return str;
        }
        throw new IllegalArgumentException("Expected [" + name + "] to be a string but was [" + value + "]");
    }

    private static SecureString extractSecureString(Map<String, Object> source, String name) {
        Object value = source.remove(name);
        if (value == null) {
            return null;
        }
        if (value instanceof String str) {
            return new SecureString(str.toCharArray());
        }
        throw new IllegalArgumentException("Expected [" + name + "] to be a string but was [" + value + "]");
    }

    private static Map<String, String> extractStringStringMap(Map<String, Object> source, String name) {
        Object value = source.remove(name);
        if (value == null) {
            return emptyMap();
        }
        if (false == value instanceof Map) {
            throw new IllegalArgumentException("Expected [" + name + "] to be an object containing strings but was [" + value + "]");
        }
        Map<?, ?> map = (Map<?, ?>) value;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (false == entry.getKey() instanceof String || false == entry.getValue() instanceof String) {
                throw new IllegalArgumentException("Expected [" + name + "] to be an object containing strings but has [" + entry + "]");
            }
        }
        @SuppressWarnings("unchecked") // We just checked....
        Map<String, String> safe = (Map<String, String>) map;
        return safe;
    }

    private static TimeValue extractTimeValue(Map<String, Object> source, String name, TimeValue defaultValue) {
        String string = extractString(source, name);
        return string == null ? defaultValue : parseTimeValue(string, name);
    }

    static void setMaxDocsValidateIdentical(AbstractBulkByScrollRequest<?> request, int maxDocs) {
        if (request.getMaxDocs() != AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES && request.getMaxDocs() != maxDocs) {
            throw new IllegalArgumentException(
                "[max_docs] set to two different values [" + request.getMaxDocs() + "]" + " and [" + maxDocs + "]"
            );
        } else {
            request.setMaxDocs(maxDocs);
        }
    }
}
