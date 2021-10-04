/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

public class CanMatchNodeResponse extends TransportResponse {

    private final List<SearchService.CanMatchResponse> responses;
    private final List<Exception> failures;

    public CanMatchNodeResponse(StreamInput in) throws IOException {
        super(in);
        responses = in.readList(i -> i.readOptionalWriteable(SearchService.CanMatchResponse::new));
        failures = in.readList(StreamInput::readException);
    }

    public CanMatchNodeResponse(List<SearchService.CanMatchResponse> responses, List<Exception> failures) {
        this.responses = responses;
        this.failures = failures;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(responses, StreamOutput::writeOptionalWriteable);
        out.writeCollection(failures, StreamOutput::writeException);
    }

    public List<SearchService.CanMatchResponse> getResponses() {
        return responses;
    }

    public List<Exception> getFailures() {
        return failures;
    }
}
