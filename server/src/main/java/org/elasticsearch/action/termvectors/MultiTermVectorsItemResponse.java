/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * A single multi get response.
 */
public class MultiTermVectorsItemResponse implements Writeable {

    private final TermVectorsResponse response;
    private final MultiTermVectorsResponse.Failure failure;

    public MultiTermVectorsItemResponse(TermVectorsResponse response, MultiTermVectorsResponse.Failure failure) {
        assert (((response == null) && (failure != null)) || ((response != null) && (failure == null)));
        this.response = response;
        this.failure = failure;
    }

    MultiTermVectorsItemResponse(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            failure = new MultiTermVectorsResponse.Failure(in);
            response = null;
        } else {
            response = new TermVectorsResponse(in);
            failure = null;
        }
    }

    /**
     * The index name of the document.
     */
    public String getIndex() {
        if (failure != null) {
            return failure.getIndex();
        }
        return response.getIndex();
    }

    /**
     * The id of the document.
     */
    public String getId() {
        if (failure != null) {
            return failure.getId();
        }
        return response.getId();
    }

    /**
     * Is this a failed execution?
     */
    public boolean isFailed() {
        return failure != null;
    }

    /**
     * The actual get response, {@code null} if its a failure.
     */
    public TermVectorsResponse getResponse() {
        return this.response;
    }

    /**
     * The failure if relevant.
     */
    public MultiTermVectorsResponse.Failure getFailure() {
        return this.failure;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (failure != null) {
            out.writeBoolean(true);
            failure.writeTo(out);
        } else {
            out.writeBoolean(false);
            response.writeTo(out);
        }
    }
}
