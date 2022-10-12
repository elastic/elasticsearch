/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xcontent.ObjectParser.fromList;

public class RestFieldCapabilitiesAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_field_caps"),
            new Route(POST, "/_field_caps"),
            new Route(GET, "/{index}/_field_caps"),
            new Route(POST, "/{index}/_field_caps")
        );
    }

    @Override
    public String getName() {
        return "field_capabilities_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final FieldCapabilitiesRequest fieldRequest = new FieldCapabilitiesRequest();
        fieldRequest.indices(indices);

        fieldRequest.indicesOptions(IndicesOptions.fromRequest(request, fieldRequest.indicesOptions()));
        fieldRequest.includeUnmapped(request.paramAsBoolean("include_unmapped", false));
        fieldRequest.filters(request.paramAsStringArray("filters", Strings.EMPTY_ARRAY));
        fieldRequest.types(request.paramAsStringArray("types", Strings.EMPTY_ARRAY));
        request.withContentOrSourceParamParserOrNull(parser -> {
            if (parser != null) {
                PARSER.parse(parser, fieldRequest, null);
            }
        });
        if (request.hasParam("fields")) {
            if (fieldRequest.fields().length > 0) {
                throw new IllegalArgumentException(
                    "can't specify a request body and [fields]"
                        + " request parameter, either specify a request body or the"
                        + " [fields] request parameter"
                );
            }
            fieldRequest.fields(Strings.splitStringByCommaToArray(request.param("fields")));
        }
        return channel -> client.fieldCaps(fieldRequest, new RestChunkedToXContentListener<>(channel));
    }

    private static final ParseField INDEX_FILTER_FIELD = new ParseField("index_filter");
    private static final ParseField RUNTIME_MAPPINGS_FIELD = new ParseField("runtime_mappings");
    private static final ParseField FIELDS_FIELD = new ParseField("fields");

    private static final ObjectParser<FieldCapabilitiesRequest, Void> PARSER = new ObjectParser<>("field_caps_request");

    static {
        PARSER.declareObject(FieldCapabilitiesRequest::indexFilter, (p, c) -> parseInnerQueryBuilder(p), INDEX_FILTER_FIELD);
        PARSER.declareObject(FieldCapabilitiesRequest::runtimeFields, (p, c) -> p.map(), RUNTIME_MAPPINGS_FIELD);
        PARSER.declareStringArray(fromList(String.class, FieldCapabilitiesRequest::fields), FIELDS_FIELD);
    }
}
