/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestCreateIndexAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestCreateIndexAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in create "
        + "index requests is deprecated. The parameter will be removed in the next major version.";

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/{index}"));
    }

    @Override
    public String getName() {
        return "create_index_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        CreateIndexRequest createIndexRequest;
        if (request.getRestApiVersion() == RestApiVersion.V_7) {
            createIndexRequest = prepareRequestV7(request);
        } else {
            createIndexRequest = prepareRequest(request);
        }
        return channel -> client.admin().indices().create(createIndexRequest, new RestToXContentListener<>(channel));
    }

    // default scope for testing types in mapping
    static CreateIndexRequest prepareRequestV7(RestRequest request) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(request.param("index"));
        if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            request.param(INCLUDE_TYPE_NAME_PARAMETER);// just consume, it is always replaced with _doc
            deprecationLogger.compatibleCritical("create_index_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        if (request.hasContent()) {
            Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false, request.getXContentType()).v2();

            sourceAsMap = prepareMappingsV7(sourceAsMap, request);

            createIndexRequest.source(sourceAsMap, LoggingDeprecationHandler.INSTANCE);
        }

        createIndexRequest.timeout(request.paramAsTime("timeout", createIndexRequest.timeout()));
        createIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", createIndexRequest.masterNodeTimeout()));
        createIndexRequest.waitForActiveShards(ActiveShardCount.parseString(request.param("wait_for_active_shards")));
        return createIndexRequest;
    }

    static Map<String, Object> prepareMappingsV7(Map<String, Object> source, RestRequest request) {
        final boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, false);

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) source.get("mappings");

        if (includeTypeName && mappings != null && mappings.size() == 1) {
            Map<String, Object> newSource = new HashMap<>();
            newSource.putAll(source); // mappings will be overridden. Aliases, settings stay the same
            String typeName = mappings.keySet().iterator().next();
            if (Strings.hasText(typeName) == false) {
                throw new IllegalArgumentException("name cannot be empty string");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> typedMappings = (Map<String, Object>) mappings.get(typeName);

            // no matter what the type was, replace it with _doc, because the internal representation still uses single type `_doc`.
            newSource.put("mappings", Collections.singletonMap(MapperService.SINGLE_MAPPING_NAME, typedMappings));
            return newSource;
        } else {
            return prepareMappings(source);
        }
    }

    static CreateIndexRequest prepareRequest(RestRequest request) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(request.param("index"));

        if (request.hasContent()) {
            Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false, request.getXContentType()).v2();
            sourceAsMap = prepareMappings(sourceAsMap);
            createIndexRequest.source(sourceAsMap, LoggingDeprecationHandler.INSTANCE);
        }

        createIndexRequest.timeout(request.paramAsTime("timeout", createIndexRequest.timeout()));
        createIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", createIndexRequest.masterNodeTimeout()));
        createIndexRequest.waitForActiveShards(ActiveShardCount.parseString(request.param("wait_for_active_shards")));

        return createIndexRequest;
    }

    static Map<String, Object> prepareMappings(Map<String, Object> source) {
        if (source.containsKey("mappings") == false || (source.get("mappings") instanceof Map) == false) {
            return source;
        }

        Map<String, Object> newSource = new HashMap<>(source);

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) source.get("mappings");
        if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, mappings)) {
            throw new IllegalArgumentException(
                "The mapping definition cannot be nested under a type "
                    + "["
                    + MapperService.SINGLE_MAPPING_NAME
                    + "] unless include_type_name is set to true."
            );
        }

        newSource.put("mappings", singletonMap(MapperService.SINGLE_MAPPING_NAME, mappings));
        return newSource;
    }
}
