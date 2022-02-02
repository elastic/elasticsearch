/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * This class parses the json request and translates it into a
 * TermVectorsRequest.
 */
public class RestTermVectorsAction extends BaseRestHandler {
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] " + "Specifying types in term vector requests is deprecated.";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/{index}/_termvectors"),
            new Route(POST, "/{index}/_termvectors"),
            new Route(GET, "/{index}/_termvectors/{id}"),
            new Route(POST, "/{index}/_termvectors/{id}"),
            Route.builder(GET, "/{index}/{type}/_termvectors").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(POST, "/{index}/{type}/_termvectors").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(GET, "/{index}/{type}/{id}/_termvectors").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(POST, "/{index}/{type}/{id}/_termvectors").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "document_term_vectors_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("type")) {
            request.param("type");
        }
        TermVectorsRequest termVectorsRequest;
        termVectorsRequest = new TermVectorsRequest(request.param("index"), request.param("id"));

        if (request.hasContentOrSourceParam()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                TermVectorsRequest.parseRequest(termVectorsRequest, parser, request.getRestApiVersion());
            }
        }
        readURIParameters(termVectorsRequest, request);

        return channel -> client.termVectors(termVectorsRequest, new RestToXContentListener<>(channel));
    }

    public static void readURIParameters(TermVectorsRequest termVectorsRequest, RestRequest request) {
        String fields = request.param("fields");
        addFieldStringsFromParameter(termVectorsRequest, fields);
        termVectorsRequest.offsets(request.paramAsBoolean("offsets", termVectorsRequest.offsets()));
        termVectorsRequest.positions(request.paramAsBoolean("positions", termVectorsRequest.positions()));
        termVectorsRequest.payloads(request.paramAsBoolean("payloads", termVectorsRequest.payloads()));
        termVectorsRequest.routing(request.param("routing"));
        termVectorsRequest.realtime(request.paramAsBoolean("realtime", termVectorsRequest.realtime()));
        termVectorsRequest.version(RestActions.parseVersion(request, termVectorsRequest.version()));
        termVectorsRequest.versionType(VersionType.fromString(request.param("version_type"), termVectorsRequest.versionType()));
        termVectorsRequest.preference(request.param("preference"));
        termVectorsRequest.termStatistics(request.paramAsBoolean("termStatistics", termVectorsRequest.termStatistics()));
        termVectorsRequest.termStatistics(request.paramAsBoolean("term_statistics", termVectorsRequest.termStatistics()));
        termVectorsRequest.fieldStatistics(request.paramAsBoolean("fieldStatistics", termVectorsRequest.fieldStatistics()));
        termVectorsRequest.fieldStatistics(request.paramAsBoolean("field_statistics", termVectorsRequest.fieldStatistics()));
    }

    public static void addFieldStringsFromParameter(TermVectorsRequest termVectorsRequest, String fields) {
        Set<String> selectedFields = termVectorsRequest.selectedFields();
        if (fields != null) {
            String[] paramFieldStrings = Strings.commaDelimitedListToStringArray(fields);
            for (String field : paramFieldStrings) {
                if (selectedFields == null) {
                    selectedFields = new HashSet<>();
                }
                if (selectedFields.contains(field) == false) {
                    field = field.replaceAll("\\s", "");
                    selectedFields.add(field);
                }
            }
        }
        if (selectedFields != null) {
            termVectorsRequest.selectedFields(selectedFields.toArray(new String[selectedFields.size()]));
        }
    }

}
