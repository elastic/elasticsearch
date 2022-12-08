/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;

final class RequestConverters {
    static final XContentType REQUEST_BODY_CONTENT_TYPE = XContentType.JSON;

    private RequestConverters() {
        // Contains only status utility methods
    }

    static Request bulk(BulkRequest bulkRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_bulk");

        Params parameters = new Params();
        parameters.withTimeout(bulkRequest.timeout());
        parameters.withRefreshPolicy(bulkRequest.getRefreshPolicy());
        parameters.withPipeline(bulkRequest.pipeline());
        parameters.withRouting(bulkRequest.routing());
        // Bulk API only supports newline delimited JSON or Smile. Before executing
        // the bulk, we need to check that all requests have the same content-type
        // and this content-type is supported by the Bulk API.
        XContentType bulkContentType = null;
        for (int i = 0; i < bulkRequest.numberOfActions(); i++) {
            DocWriteRequest<?> action = bulkRequest.requests().get(i);

            DocWriteRequest.OpType opType = action.opType();
            if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                bulkContentType = enforceSameContentType((IndexRequest) action, bulkContentType);

            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                UpdateRequest updateRequest = (UpdateRequest) action;
                if (updateRequest.doc() != null) {
                    bulkContentType = enforceSameContentType(updateRequest.doc(), bulkContentType);
                }
                if (updateRequest.upsertRequest() != null) {
                    bulkContentType = enforceSameContentType(updateRequest.upsertRequest(), bulkContentType);
                }
            }
        }

        if (bulkContentType == null) {
            bulkContentType = XContentType.JSON;
        }

        final byte separator = bulkContentType.xContent().streamSeparator();
        final ContentType requestContentType = createContentType(bulkContentType);

        ByteArrayOutputStream content = new ByteArrayOutputStream();
        for (DocWriteRequest<?> action : bulkRequest.requests()) {
            DocWriteRequest.OpType opType = action.opType();

            try (XContentBuilder metadata = XContentBuilder.builder(bulkContentType.xContent())) {
                metadata.startObject();
                {
                    metadata.startObject(opType.getLowercase());
                    if (Strings.hasLength(action.index())) {
                        metadata.field("_index", action.index());
                    }
                    if (Strings.hasLength(action.id())) {
                        metadata.field("_id", action.id());
                    }
                    if (Strings.hasLength(action.routing())) {
                        metadata.field("routing", action.routing());
                    }
                    if (action.version() != Versions.MATCH_ANY) {
                        metadata.field("version", action.version());
                    }

                    VersionType versionType = action.versionType();
                    if (versionType != VersionType.INTERNAL) {
                        if (versionType == VersionType.EXTERNAL) {
                            metadata.field("version_type", "external");
                        } else if (versionType == VersionType.EXTERNAL_GTE) {
                            metadata.field("version_type", "external_gte");
                        }
                    }

                    if (action.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        metadata.field("if_seq_no", action.ifSeqNo());
                        metadata.field("if_primary_term", action.ifPrimaryTerm());
                    }

                    if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                        IndexRequest indexRequest = (IndexRequest) action;
                        if (Strings.hasLength(indexRequest.getPipeline())) {
                            metadata.field("pipeline", indexRequest.getPipeline());
                        }
                    } else if (opType == DocWriteRequest.OpType.UPDATE) {
                        UpdateRequest updateRequest = (UpdateRequest) action;
                        if (updateRequest.retryOnConflict() > 0) {
                            metadata.field("retry_on_conflict", updateRequest.retryOnConflict());
                        }
                        if (updateRequest.fetchSource() != null) {
                            metadata.field("_source", updateRequest.fetchSource());
                        }
                    }
                    metadata.endObject();
                }
                metadata.endObject();

                BytesRef metadataSource = BytesReference.bytes(metadata).toBytesRef();
                content.write(metadataSource.bytes, metadataSource.offset, metadataSource.length);
                content.write(separator);
            }

            BytesRef source = null;
            if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                IndexRequest indexRequest = (IndexRequest) action;
                BytesReference indexSource = indexRequest.source();
                XContentType indexXContentType = indexRequest.getContentType();

                try (
                    XContentParser parser = XContentHelper.createParser(
                        /*
                         * EMPTY and THROW are fine here because we just call
                         * copyCurrentStructure which doesn't touch the
                         * registry or deprecation.
                         */
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        indexSource,
                        indexXContentType
                    )
                ) {
                    try (XContentBuilder builder = XContentBuilder.builder(bulkContentType.xContent())) {
                        builder.copyCurrentStructure(parser);
                        source = BytesReference.bytes(builder).toBytesRef();
                    }
                }
            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                source = XContentHelper.toXContent((UpdateRequest) action, bulkContentType, false).toBytesRef();
            }

            if (source != null) {
                content.write(source.bytes, source.offset, source.length);
                content.write(separator);
            }
        }
        request.addParameters(parameters.asMap());
        request.setEntity(new NByteArrayEntity(content.toByteArray(), 0, content.size(), requestContentType));
        return request;
    }

    static Request index(IndexRequest indexRequest) {
        String method = Strings.hasLength(indexRequest.id()) ? HttpPut.METHOD_NAME : HttpPost.METHOD_NAME;

        String endpoint;
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            endpoint = endpoint(indexRequest.index(), "_create", indexRequest.id());
        } else {
            endpoint = endpoint(indexRequest.index(), indexRequest.id());
        }

        Request request = new Request(method, endpoint);

        Params parameters = new Params();
        parameters.withRouting(indexRequest.routing());
        parameters.withTimeout(indexRequest.timeout());
        parameters.withVersion(indexRequest.version());
        parameters.withVersionType(indexRequest.versionType());
        parameters.withIfSeqNo(indexRequest.ifSeqNo());
        parameters.withIfPrimaryTerm(indexRequest.ifPrimaryTerm());
        parameters.withPipeline(indexRequest.getPipeline());
        parameters.withRefreshPolicy(indexRequest.getRefreshPolicy());
        parameters.withWaitForActiveShards(indexRequest.waitForActiveShards());
        parameters.withRequireAlias(indexRequest.isRequireAlias());

        BytesRef source = indexRequest.source().toBytesRef();
        ContentType contentType = createContentType(indexRequest.getContentType());
        request.addParameters(parameters.asMap());
        request.setEntity(new NByteArrayEntity(source.bytes, source.offset, source.length, contentType));
        return request;
    }

    /**
     * Convert a {@linkplain SearchRequest} into a {@linkplain Request}.
     * @param searchRequest the request to convert
     * @param searchEndpoint the name of the search endpoint. {@literal _search}
     *    for standard searches and {@literal _rollup_search} for rollup
     *    searches.
     */
    static Request search(SearchRequest searchRequest, String searchEndpoint) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, endpoint(searchRequest.indices(), searchEndpoint));

        Params params = new Params();
        addSearchRequestParams(params, searchRequest);

        if (searchRequest.source() != null) {
            request.setEntity(createEntity(searchRequest.source(), REQUEST_BODY_CONTENT_TYPE));
        }
        request.addParameters(params.asMap());
        return request;
    }

    static void addSearchRequestParams(Params params, SearchRequest searchRequest) {
        params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
        params.withRouting(searchRequest.routing());
        params.withPreference(searchRequest.preference());
        if (SearchRequest.DEFAULT_INDICES_OPTIONS.equals(searchRequest.indicesOptions()) == false) {
            params.withIndicesOptions(searchRequest.indicesOptions());
        }
        params.withSearchType(searchRequest.searchType().name().toLowerCase(Locale.ROOT));
        if (searchRequest.isCcsMinimizeRoundtrips() != SearchRequest.defaultCcsMinimizeRoundtrips(searchRequest)) {
            params.putParam("ccs_minimize_roundtrips", Boolean.toString(searchRequest.isCcsMinimizeRoundtrips()));
        }
        if (searchRequest.getPreFilterShardSize() != null) {
            params.putParam("pre_filter_shard_size", Integer.toString(searchRequest.getPreFilterShardSize()));
        }
        params.withMaxConcurrentShardRequests(searchRequest.getMaxConcurrentShardRequests());
        if (searchRequest.requestCache() != null) {
            params.withRequestCache(searchRequest.requestCache());
        }
        if (searchRequest.allowPartialSearchResults() != null) {
            params.withAllowPartialResults(searchRequest.allowPartialSearchResults());
        }
        params.withBatchedReduceSize(searchRequest.getBatchedReduceSize());
        if (searchRequest.scroll() != null) {
            params.putParam("scroll", searchRequest.scroll().keepAlive());
        }
    }

    static Request searchScroll(SearchScrollRequest searchScrollRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_search/scroll");
        request.setEntity(createEntity(searchScrollRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static HttpEntity createEntity(ToXContent toXContent, XContentType xContentType) throws IOException {
        return createEntity(toXContent, xContentType, ToXContent.EMPTY_PARAMS);
    }

    static HttpEntity createEntity(ToXContent toXContent, XContentType xContentType, ToXContent.Params toXContentParams)
        throws IOException {
        BytesRef source = XContentHelper.toXContent(toXContent, xContentType, toXContentParams, false).toBytesRef();
        return new NByteArrayEntity(source.bytes, source.offset, source.length, createContentType(xContentType));
    }

    @Deprecated
    static String endpoint(String index, String type, String id) {
        return new EndpointBuilder().addPathPart(index, type, id).build();
    }

    static String endpoint(String index, String id) {
        return new EndpointBuilder().addPathPart(index, "_doc", id).build();
    }

    @Deprecated
    static String endpoint(String index, String type, String id, String endpoint) {
        return new EndpointBuilder().addPathPart(index, type, id).addPathPartAsIs(endpoint).build();
    }

    static String endpoint(String[] indices, String endpoint) {
        return new EndpointBuilder().addCommaSeparatedPathParts(indices).addPathPartAsIs(endpoint).build();
    }

    @Deprecated
    static String endpoint(String[] indices, String[] types, String endpoint) {
        return new EndpointBuilder().addCommaSeparatedPathParts(indices)
            .addCommaSeparatedPathParts(types)
            .addPathPartAsIs(endpoint)
            .build();
    }

    @Deprecated
    static String endpoint(String[] indices, String endpoint, String type) {
        return new EndpointBuilder().addCommaSeparatedPathParts(indices).addPathPartAsIs(endpoint).addPathPart(type).build();
    }

    /**
     * Returns a {@link ContentType} from a given {@link XContentType}.
     *
     * @param xContentType the {@link XContentType}
     * @return the {@link ContentType}
     */
    @SuppressForbidden(reason = "Only allowed place to convert a XContentType to a ContentType")
    public static ContentType createContentType(final XContentType xContentType) {
        return ContentType.create(xContentType.mediaTypeWithoutParameters(), (Charset) null);
    }

    /**
     * Utility class to help with common parameter names and patterns. Wraps
     * a {@link Request} and adds the parameters to it directly.
     */
    static class Params {
        private final Map<String, String> parameters = new HashMap<>();

        Params() {}

        Params putParam(String name, String value) {
            if (Strings.hasLength(value)) {
                parameters.put(name, value);
            }
            return this;
        }

        Params putParam(String key, TimeValue value) {
            if (value != null) {
                return putParam(key, value.getStringRep());
            }
            return this;
        }

        Map<String, String> asMap() {
            return parameters;
        }

        Params withPipeline(String pipeline) {
            return putParam("pipeline", pipeline);
        }

        Params withPreference(String preference) {
            return putParam("preference", preference);
        }

        Params withSearchType(String searchType) {
            return putParam("search_type", searchType);
        }

        Params withMaxConcurrentShardRequests(int maxConcurrentShardRequests) {
            return putParam("max_concurrent_shard_requests", Integer.toString(maxConcurrentShardRequests));
        }

        Params withBatchedReduceSize(int batchedReduceSize) {
            return putParam("batched_reduce_size", Integer.toString(batchedReduceSize));
        }

        Params withRequestCache(boolean requestCache) {
            return putParam("request_cache", Boolean.toString(requestCache));
        }

        Params withAllowPartialResults(boolean allowPartialSearchResults) {
            return putParam("allow_partial_search_results", Boolean.toString(allowPartialSearchResults));
        }

        Params withRefreshPolicy(RefreshPolicy refreshPolicy) {
            if (refreshPolicy != RefreshPolicy.NONE) {
                return putParam("refresh", refreshPolicy.getValue());
            }
            return this;
        }

        Params withRouting(String routing) {
            return putParam("routing", routing);
        }

        Params withTimeout(TimeValue timeout) {
            return putParam("timeout", timeout);
        }

        Params withVersion(long version) {
            if (version != Versions.MATCH_ANY) {
                return putParam("version", Long.toString(version));
            }
            return this;
        }

        Params withVersionType(VersionType versionType) {
            if (versionType != VersionType.INTERNAL) {
                return putParam("version_type", versionType.name().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Params withIfSeqNo(long ifSeqNo) {
            if (ifSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                return putParam("if_seq_no", Long.toString(ifSeqNo));
            }
            return this;
        }

        Params withIfPrimaryTerm(long ifPrimaryTerm) {
            if (ifPrimaryTerm != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                return putParam("if_primary_term", Long.toString(ifPrimaryTerm));
            }
            return this;
        }

        Params withWaitForActiveShards(ActiveShardCount activeShardCount) {
            return withWaitForActiveShards(activeShardCount, ActiveShardCount.DEFAULT);
        }

        Params withWaitForActiveShards(ActiveShardCount activeShardCount, ActiveShardCount defaultActiveShardCount) {
            if (activeShardCount != null && activeShardCount != defaultActiveShardCount) {
                return putParam("wait_for_active_shards", activeShardCount.toString().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Params withRequireAlias(boolean requireAlias) {
            if (requireAlias) {
                return putParam("require_alias", Boolean.toString(requireAlias));
            }
            return this;
        }

        Params withIndicesOptions(IndicesOptions indicesOptions) {
            if (indicesOptions != null) {
                withIgnoreUnavailable(indicesOptions.ignoreUnavailable());
                putParam("allow_no_indices", Boolean.toString(indicesOptions.allowNoIndices()));
                String expandWildcards;
                if (indicesOptions.expandWildcardExpressions() == false) {
                    expandWildcards = "none";
                } else {
                    StringJoiner joiner = new StringJoiner(",");
                    if (indicesOptions.expandWildcardsOpen()) {
                        joiner.add("open");
                    }
                    if (indicesOptions.expandWildcardsClosed()) {
                        joiner.add("closed");
                    }
                    expandWildcards = joiner.toString();
                }
                putParam("expand_wildcards", expandWildcards);
                putParam("ignore_throttled", Boolean.toString(indicesOptions.ignoreThrottled()));
            }
            return this;
        }

        Params withIgnoreUnavailable(boolean ignoreUnavailable) {
            // Always explicitly place the ignore_unavailable value.
            putParam("ignore_unavailable", Boolean.toString(ignoreUnavailable));
            return this;
        }
    }

    /**
     * Ensure that the {@link IndexRequest}'s content type is supported by the Bulk API and that it conforms
     * to the current {@link BulkRequest}'s content type (if it's known at the time of this method get called).
     *
     * @return the {@link IndexRequest}'s content type
     */
    static XContentType enforceSameContentType(IndexRequest indexRequest, @Nullable XContentType xContentType) {
        XContentType requestContentType = indexRequest.getContentType();
        if (requestContentType.canonical() != XContentType.JSON && requestContentType.canonical() != XContentType.SMILE) {
            throw new IllegalArgumentException(
                "Unsupported content-type found for request with content-type ["
                    + requestContentType
                    + "], only JSON and SMILE are supported"
            );
        }
        if (xContentType == null) {
            return requestContentType;
        }
        if (requestContentType.canonical() != xContentType.canonical()) {
            throw new IllegalArgumentException(
                "Mismatching content-type found for request with content-type ["
                    + requestContentType
                    + "], previous requests have content-type ["
                    + xContentType
                    + "]"
            );
        }
        return xContentType;
    }

    /**
     * Utility class to build request's endpoint given its parts as strings
     */
    static class EndpointBuilder {

        private final StringJoiner joiner = new StringJoiner("/", "/", "");

        EndpointBuilder addPathPart(String... parts) {
            for (String part : parts) {
                if (Strings.hasLength(part)) {
                    joiner.add(encodePart(part));
                }
            }
            return this;
        }

        EndpointBuilder addCommaSeparatedPathParts(String[] parts) {
            addPathPart(String.join(",", parts));
            return this;
        }

        EndpointBuilder addPathPartAsIs(String... parts) {
            for (String part : parts) {
                if (Strings.hasLength(part)) {
                    joiner.add(part);
                }
            }
            return this;
        }

        String build() {
            return joiner.toString();
        }

        private static String encodePart(String pathPart) {
            try {
                // encode each part (e.g. index, type and id) separately before merging them into the path
                // we prepend "/" to the path part to make this path absolute, otherwise there can be issues with
                // paths that start with `-` or contain `:`
                // the authority must be an empty string and not null, else paths that being with slashes could have them
                // misinterpreted as part of the authority.
                URI uri = new URI(null, "", "/" + pathPart, null, null);
                // manually encode any slash that each part may contain
                return uri.getRawPath().substring(1).replaceAll("/", "%2F");
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Path part [" + pathPart + "] couldn't be encoded", e);
            }
        }
    }
}
