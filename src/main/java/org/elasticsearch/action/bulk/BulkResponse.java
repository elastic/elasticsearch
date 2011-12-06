/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.bulk;

import com.google.common.collect.Iterators;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Iterator;

/**
 * A response of a bulk execution. Holding a response for each item responding (in order) of the
 * bulk requests. Each item holds the index/type/id is operated on, and if it failed or not (with the
 * failure message).
 *
 *
 */
public class BulkResponse implements ActionResponse, Iterable<BulkItemResponse> {

    private BulkItemResponse[] responses;

    private long tookInMillis;

    BulkResponse() {
    }

    public BulkResponse(BulkItemResponse[] responses, long tookInMillis) {
        this.responses = responses;
        this.tookInMillis = tookInMillis;
    }

    /**
     * How long the bulk execution took.
     */
    public TimeValue took() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the bulk execution took.
     */
    public TimeValue getTook() {
        return took();
    }

    /**
     * How long the bulk execution took in milliseconds.
     */
    public long tookInMillis() {
        return tookInMillis;
    }

    /**
     * How long the bulk execution took in milliseconds.
     */
    public long getTookInMillis() {
        return tookInMillis();
    }

    /**
     * Has anything failed with the execution.
     */
    public boolean hasFailures() {
        for (BulkItemResponse response : responses) {
            if (response.failed()) {
                return true;
            }
        }
        return false;
    }

    public String buildFailureMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("failure in bulk execution:");
        for (int i = 0; i < responses.length; i++) {
            BulkItemResponse response = responses[i];
            if (response.failed()) {
                sb.append("\n[").append(i)
                        .append("]: index [").append(response.index()).append("], type [").append(response.type()).append("], id [").append(response.id())
                        .append("], message [").append(response.failureMessage()).append("]");
            }
        }
        return sb.toString();
    }

    /**
     * The items representing each action performed in the bulk operation (in the same order!).
     */
    public BulkItemResponse[] items() {
        return responses;
    }

    @Override
    public Iterator<BulkItemResponse> iterator() {
        return Iterators.forArray(responses);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        responses = new BulkItemResponse[in.readVInt()];
        for (int i = 0; i < responses.length; i++) {
            responses[i] = BulkItemResponse.readBulkItem(in);
        }
        tookInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(responses.length);
        for (BulkItemResponse response : responses) {
            response.writeTo(out);
        }
        out.writeVLong(tookInMillis);
    }
}
