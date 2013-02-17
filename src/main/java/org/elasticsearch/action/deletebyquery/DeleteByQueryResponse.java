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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * The response of delete by query action. Holds the {@link IndexDeleteByQueryResponse}s from all the
 * different indices.
 */
public class DeleteByQueryResponse extends ActionResponse implements Iterable<IndexDeleteByQueryResponse> {

    private Map<String, IndexDeleteByQueryResponse> indices = newHashMap();

    DeleteByQueryResponse() {

    }

    @Override
    public Iterator<IndexDeleteByQueryResponse> iterator() {
        return indices.values().iterator();
    }

    /**
     * The responses from all the different indices.
     */
    public Map<String, IndexDeleteByQueryResponse> getIndices() {
        return indices;
    }

    /**
     * The response of a specific index.
     */
    public IndexDeleteByQueryResponse getIndex(String index) {
        return indices.get(index);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            IndexDeleteByQueryResponse response = new IndexDeleteByQueryResponse();
            response.readFrom(in);
            indices.put(response.getIndex(), response);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indices.size());
        for (IndexDeleteByQueryResponse indexResponse : indices.values()) {
            indexResponse.writeTo(out);
        }
    }
}
