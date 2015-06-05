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
 */
public class BulkResponse extends ActionResponse implements Iterable<BulkItemResponse> {

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
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the bulk execution took in milliseconds.
     */
    public long getTookInMillis() {
        return tookInMillis;
    }

    /**
     * Has anything failed with the execution.
     */
    public boolean hasFailures() {
        for (BulkItemResponse response : responses) {
            if (response.isFailed()) {
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
            if (response.isFailed()) {
                sb.append("\n[").append(i)
                        .append("]: index [").append(response.getIndex()).append("], type [").append(response.getType()).append("], id [").append(response.getId())
                        .append("], message [").append(response.getFailureMessage()).append("]");
            }
        }
        return sb.toString();
    }

    /**
     * The items representing each action performed in the bulk operation (in the same order!).
     */
    public BulkItemResponse[] getItems() {
        return responses;
    }

    @Override
    public Iterator<BulkItemResponse> iterator() {
        return Iterators.forArray(responses);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        responses = new BulkItemResponse[in.readVInt()];
        for (int i = 0; i < responses.length; i++) {
            responses[i] = BulkItemResponse.readBulkItem(in);
        }
        tookInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(responses.length);
        for (BulkItemResponse response : responses) {
            response.writeTo(out);
        }
        out.writeVLong(tookInMillis);
    }
}
