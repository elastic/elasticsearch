/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Iterator;

/**
 * A response of a bulk execution. Holding a response for each item responding (in order) of the
 * bulk requests.
 *
 * @author kimchy (shay.banon)
 */
public class BulkResponse implements ActionResponse, Iterable<BulkItemResponse> {

    private BulkItemResponse[] responses;

    BulkResponse() {
    }

    public BulkResponse(BulkItemResponse[] responses) {
        this.responses = responses;
    }

    public boolean hasFailures() {
        for (BulkItemResponse response : responses) {
            if (response.failed()) {
                return true;
            }
        }
        return false;
    }

    public BulkItemResponse[] items() {
        return responses;
    }

    @Override public Iterator<BulkItemResponse> iterator() {
        return Iterators.forArray(responses);
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        responses = new BulkItemResponse[in.readVInt()];
        for (int i = 0; i < responses.length; i++) {
            responses[i] = BulkItemResponse.readBulkItem(in);
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(responses.length);
        for (BulkItemResponse response : responses) {
            response.writeTo(out);
        }
    }
}
