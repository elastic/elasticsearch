/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.DispatchingRestToXContentListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class RestGetMappingAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestGetMappingAction.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in get"
        + " mapping requests is deprecated. The parameter will be removed in the next major version.";

    private final ThreadPool threadPool;

    public RestGetMappingAction(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_mapping"),
                new Route(GET, "/_mappings"),
                new Route(GET, "/{index}/{type}/_mapping"),
                new Route(GET, "/{index}/_mapping"),
                new Route(GET, "/{index}/_mappings"),
                new Route(GET, "/{index}/_mappings/{type}"),
                new Route(GET, "/{index}/_mapping/{type}"),
                new Route(HEAD, "/{index}/_mapping/{type}"),
                new Route(GET, "/_mapping/{type}")
            )
        );
    }

    @Override
    public String getName() {
        return "get_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] types = request.paramAsStringArrayOrEmptyIfAll("type");
        boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);

        if (request.method().equals(HEAD)) {
            deprecationLogger.critical(
                DeprecationCategory.TYPES,
                "get_mapping_types_removal",
                "Type exists requests are deprecated, as types have been deprecated."
            );
        } else if (includeTypeName == false && types.length > 0) {
            throw new IllegalArgumentException(
                "Types cannot be provided in get mapping requests, unless" + " include_type_name is set to true."
            );
        }
        if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            deprecationLogger.critical(DeprecationCategory.TYPES, "get_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indices).types(types);
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        final TimeValue timeout = request.paramAsTime("master_timeout", getMappingsRequest.masterNodeTimeout());
        getMappingsRequest.masterNodeTimeout(timeout);
        getMappingsRequest.local(request.paramAsBoolean("local", getMappingsRequest.local()));
        final HttpChannel httpChannel = request.getHttpChannel();
        return channel -> new RestCancellableNodeClient(client, httpChannel).admin()
            .indices()
            .getMappings(
                getMappingsRequest,
                new DispatchingRestToXContentListener<>(threadPool.executor(ThreadPool.Names.MANAGEMENT), channel, request).map(
                    getMappingsResponse -> new RestGetMappingsResponse(
                        types,
                        getMappingsResponse,
                        threadPool::relativeTimeInMillis,
                        timeout
                    )
                )
            );
    }

    private static final class RestGetMappingsResponse implements StatusToXContentObject {
        private final String[] types;
        private final GetMappingsResponse response;
        private final LongSupplier relativeTimeSupplierMillis;
        private final TimeValue timeout;
        private final long startTimeMs;
        private final SortedSet<String> missingTypes;

        RestGetMappingsResponse(String[] types, GetMappingsResponse response, LongSupplier relativeTimeSupplierMillis, TimeValue timeout) {
            this.types = types;
            this.response = response;
            this.relativeTimeSupplierMillis = relativeTimeSupplierMillis;
            this.timeout = timeout;
            this.startTimeMs = relativeTimeSupplierMillis.getAsLong();
            this.missingTypes = Collections.unmodifiableSortedSet(getMissingTypes(types, response.getMappings()));
        }

        @Override
        public RestStatus status() {
            return missingTypes.isEmpty() ? RestStatus.OK : RestStatus.NOT_FOUND;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (relativeTimeSupplierMillis.getAsLong() - startTimeMs > timeout.millis()) {
                throw new ElasticsearchTimeoutException("Timed out getting mappings");
            }

            final ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> mappingsByIndex = response.getMappings();
            if (mappingsByIndex.isEmpty() && types.length != 0) {
                builder.close();
                throw new TypeMissingException("_all", String.join(",", types));
            }

            builder.startObject();
            {
                if (missingTypes.isEmpty() == false) {
                    final String message = String.format(
                        Locale.ROOT,
                        "type" + (missingTypes.size() == 1 ? "" : "s") + " [%s] missing",
                        Strings.collectionToCommaDelimitedString(missingTypes)
                    );
                    builder.field("error", message);
                    builder.field("status", status().getStatus());
                }
                response.toXContent(builder, params);
            }
            builder.endObject();

            return builder;
        }

        private static SortedSet<String> getMissingTypes(
            String[] types,
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> mappingsByIndex
        ) {
            if (mappingsByIndex.isEmpty() && types.length != 0) {
                return Collections.emptySortedSet();
            }
            final Set<String> typeNames = new HashSet<>();
            for (final ImmutableOpenMap<String, MappingMetadata> value : mappingsByIndex.values()) {
                for (final ObjectCursor<String> inner : value.keys()) {
                    typeNames.add(inner.value);
                }
            }

            final SortedSet<String> difference = Sets.sortedDifference(Arrays.stream(types).collect(Collectors.toSet()), typeNames);

            // now remove requested aliases that contain wildcards that are simple matches
            final List<String> matches = new ArrayList<>();
            outer: for (final String pattern : difference) {
                if (pattern.contains("*")) {
                    for (final String typeName : typeNames) {
                        if (Regex.simpleMatch(pattern, typeName)) {
                            matches.add(pattern);
                            continue outer;
                        }
                    }
                }
            }
            difference.removeAll(matches);
            return difference;
        }
    }
}
