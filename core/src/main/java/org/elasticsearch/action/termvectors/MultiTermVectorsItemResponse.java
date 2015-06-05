/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 * A single multi get response.
 */
public class MultiTermVectorsItemResponse implements Streamable {

    private TermVectorsResponse response;
    private MultiTermVectorsResponse.Failure failure;

    MultiTermVectorsItemResponse() {

    }

    public MultiTermVectorsItemResponse(TermVectorsResponse response, MultiTermVectorsResponse.Failure failure) {
        assert (((response == null) && (failure != null)) || ((response != null) && (failure == null)));
        this.response = response;
        this.failure = failure;
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
     * The type of the document.
     */
    public String getType() {
        if (failure != null) {
            return failure.getType();
        }
        return response.getType();
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
     * The actual get response, <tt>null</tt> if its a failure.
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

    public static MultiTermVectorsItemResponse readItemResponse(StreamInput in) throws IOException {
        MultiTermVectorsItemResponse response = new MultiTermVectorsItemResponse();
        response.readFrom(in);
        return response;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            failure = MultiTermVectorsResponse.Failure.readFailure(in);
        } else {
            response = new TermVectorsResponse();
            response.readFrom(in);
        }
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