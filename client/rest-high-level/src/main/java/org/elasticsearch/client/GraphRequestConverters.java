/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.graph.GraphExploreRequest;

import java.io.IOException;

final class GraphRequestConverters {

    private GraphRequestConverters() {}

    static Request explore(GraphExploreRequest exploreRequest) throws IOException {
        String endpoint = RequestConverters.endpoint(exploreRequest.indices(), "_graph/explore");
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.setEntity(RequestConverters.createEntity(exploreRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }
}
