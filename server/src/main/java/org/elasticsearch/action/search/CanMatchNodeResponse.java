/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

public class CanMatchNodeResponse extends TransportResponse {

    private final List<SearchService.CanMatchResponse> responses;

    public CanMatchNodeResponse(StreamInput in) throws IOException {
        super(in);
        responses = in.readList(SearchService.CanMatchResponse::new);
    }

    public CanMatchNodeResponse(List<SearchService.CanMatchResponse> responses) {
        this.responses = responses;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(responses);
    }

    public List<SearchService.CanMatchResponse> getResponses() {
        return responses;
    }

}
