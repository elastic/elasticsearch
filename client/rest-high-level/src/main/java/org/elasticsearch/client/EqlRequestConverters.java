/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.eql.EqlSearchRequest;
import org.elasticsearch.client.eql.EqlStatsRequest;

import java.io.IOException;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.createEntity;

final class EqlRequestConverters {

    static Request search(EqlSearchRequest eqlSearchRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addCommaSeparatedPathParts(eqlSearchRequest.indices())
            .addPathPartAsIs("_eql", "search")
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withIndicesOptions(eqlSearchRequest.indicesOptions());
        request.setEntity(createEntity(eqlSearchRequest, REQUEST_BODY_CONTENT_TYPE));
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request stats(EqlStatsRequest eqlStatsRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_eql", "stats").build();
        return new Request(HttpGet.METHOD_NAME, endpoint);
    }
}
