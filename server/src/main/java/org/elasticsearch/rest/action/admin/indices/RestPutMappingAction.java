/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestPutMappingAction extends BaseRestHandler {
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in put mapping request is deprecated. "
        + "Use typeless api instead";
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPutMappingAction.class);

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/{index}/_mapping/"),
            new Route(PUT, "/{index}/_mapping/"),
            new Route(POST, "/{index}/_mappings/"),
            new Route(PUT, "/{index}/_mappings/"),
            Route.builder(POST, "/{index}/{type}/_mapping")
                .deprecated(TYPES_DEPRECATION_MESSAGE, DeprecationLogger.CRITICAL, RestApiVersion.V_7)
                .build(),
            Route.builder(PUT, "/{index}/{type}/_mapping")
                .deprecated(TYPES_DEPRECATION_MESSAGE, DeprecationLogger.CRITICAL, RestApiVersion.V_7)
                .build(),
            Route.builder(POST, "/{index}/_mapping/{type}")
                .deprecated(TYPES_DEPRECATION_MESSAGE, DeprecationLogger.CRITICAL, RestApiVersion.V_7)
                .build(),
            Route.builder(PUT, "/{index}/_mapping/{type}")
                .deprecated(TYPES_DEPRECATION_MESSAGE, DeprecationLogger.CRITICAL, RestApiVersion.V_7)
                .build(),
            Route.builder(POST, "/_mapping/{type}")
                .deprecated(TYPES_DEPRECATION_MESSAGE, DeprecationLogger.CRITICAL, RestApiVersion.V_7)
                .build(),
            Route.builder(PUT, "/_mapping/{type}")
                .deprecated(TYPES_DEPRECATION_MESSAGE, DeprecationLogger.CRITICAL, RestApiVersion.V_7)
                .build(),
            Route.builder(POST, "/{index}/{type}/_mappings")
                .deprecated(TYPES_DEPRECATION_MESSAGE, DeprecationLogger.CRITICAL, RestApiVersion.V_7)
                .build(),
            Route.builder(PUT, "/{index}/{type}/_mappings")
                .deprecated(TYPES_DEPRECATION_MESSAGE, DeprecationLogger.CRITICAL, RestApiVersion.V_7)
                .build(),
            Route.builder(POST, "/{index}/_mappings/{type}")
                .deprecated(TYPES_DEPRECATION_MESSAGE, DeprecationLogger.CRITICAL, RestApiVersion.V_7)
                .build(),
            Route.builder(PUT, "/{index}/_mappings/{type}")
                .deprecated(TYPES_DEPRECATION_MESSAGE, DeprecationLogger.CRITICAL, RestApiVersion.V_7)
                .build(),
            Route.builder(POST, "/_mappings/{type}")
                .deprecated(TYPES_DEPRECATION_MESSAGE, DeprecationLogger.CRITICAL, RestApiVersion.V_7)
                .build(),
            Route.builder(PUT, "/_mappings/{type}")
                .deprecated(TYPES_DEPRECATION_MESSAGE, DeprecationLogger.CRITICAL, RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "put_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        PutMappingRequest putMappingRequest = new PutMappingRequest(indices);

        // the mappings are only inspected with a streaming parser, never realised as a java map
        final BytesReference content = request.requiredContent();
        final XContentType xContentType = request.getXContentType();
        final String singleTopLevelKey = singleTopLevelKey(content, xContentType);
        final boolean typed = MapperService.SINGLE_MAPPING_NAME.equals(singleTopLevelKey);
        if (request.getRestApiVersion() == RestApiVersion.V_7) {
            final boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);
            if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
                deprecationLogger.compatibleCritical("put_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
            }
            final String type = request.param("type");
            if (includeTypeName == false && (type != null || typed)) {
                throw new IllegalArgumentException(
                    "Types cannot be provided in put mapping requests, unless the include_type_name parameter is set to true."
                );
            }

            if (includeTypeName && singleTopLevelKey != null) {
                putMappingRequest.source(unwrapType(singleTopLevelKey, content, xContentType), XContentType.JSON);
            } else {
                putMappingRequest.source(content, xContentType);
            }
        } else {
            if (typed) {
                throw new IllegalArgumentException("Types cannot be provided in put mapping requests");
            }
            putMappingRequest.source(content, xContentType);
        }

        putMappingRequest.ackTimeout(getAckTimeout(request));
        putMappingRequest.masterNodeTimeout(getMasterNodeTimeout(request));
        putMappingRequest.indicesOptions(IndicesOptions.fromRequest(request, putMappingRequest.indicesOptions()));
        putMappingRequest.writeIndexOnly(request.paramAsBoolean("write_index_only", false));
        return channel -> client.admin().indices().putMapping(putMappingRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Returns the name of the only top-level field of the mappings, or {@code null} if the mappings
     * do not have exactly one top-level field. Nested content is skipped rather than parsed.
     */
    private static String singleTopLevelKey(BytesReference content, XContentType xContentType) throws IOException {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, content, xContentType)) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("mapping source must be an object");
            }
            if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
                return null;
            }
            final String key = parser.currentName();
            parser.nextToken();
            parser.skipChildren();
            return parser.nextToken() == XContentParser.Token.END_OBJECT ? key : null;
        }
    }

    /**
     * Copies the value of the single top-level field, which is the typeless mappings wrapped in a type name.
     */
    private static BytesReference unwrapType(String typeName, BytesReference content, XContentType xContentType) throws IOException {
        if (Strings.hasText(typeName) == false) {
            throw new IllegalArgumentException("name cannot be empty string");
        }
        try (
            XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, content, xContentType);
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)
        ) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("mappings for type [" + typeName + "] must be an object");
            }
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

}
