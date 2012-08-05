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

package org.elasticsearch.action.updatebyquery;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Encapsulates the result of an update by query request by bundling all bulk item responses.
 * Each bulk item response holds the result of an individual update.
 */
public class UpdateByQueryResponse implements ActionResponse {

    private long tookInMillis;
    private long totalHits;
    private long updated;
    private IndexUpdateByQueryResponse[] indexResponses = new IndexUpdateByQueryResponse[0];
    private String[] mainFailures = Strings.EMPTY_ARRAY;

    UpdateByQueryResponse() {
    }

    public UpdateByQueryResponse(long tookInMillis, IndexUpdateByQueryResponse... indexResponses) {
        this.tookInMillis = tookInMillis;
        indexResponses(indexResponses);
    }

    public long tookInMillis() {
        return tookInMillis;
    }

    public long getTookInMillis() {
        return tookInMillis();
    }

    public IndexUpdateByQueryResponse[] indexResponses() {
        return indexResponses;
    }

    public IndexUpdateByQueryResponse[] getIndexResponses() {
        return indexResponses();
    }

    public UpdateByQueryResponse indexResponses(IndexUpdateByQueryResponse[] responses) {
        for (IndexUpdateByQueryResponse response : responses) {
            totalHits += response.totalHits();
            updated += response.updated();
        }
        this.indexResponses = responses;
        return this;
    }

    /**
     * @return The main index level failures
     */
    public String[] mainFailures() {
        return mainFailures;
    }

    public String[] getMainFailures() {
        return mainFailures();
    }

    public UpdateByQueryResponse mainFailures(String[] mainFailures) {
        this.mainFailures = mainFailures;
        return this;
    }

    /**
     * @return the number of documents that have matched with the update query
     */
    public long totalHits() {
        return totalHits;
    }

    public long getTotalHits() {
        return totalHits();
    }

    /**
     * @return the number of documents that are actually updated
     */
    public long updated() {
        return updated;
    }

    public long getUpdated() {
        return updated();
    }

    public boolean hasFailures() {
        return mainFailures().length != 0;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        tookInMillis = in.readVLong();
        totalHits = in.readVLong();
        updated = in.readVLong();
        indexResponses = new IndexUpdateByQueryResponse[in.readVInt()];
        for (int i = 0; i < indexResponses.length; i++) {
            indexResponses[i] = IndexUpdateByQueryResponse.readResponseItem(in);
        }
        mainFailures = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(tookInMillis);
        out.writeVLong(totalHits);
        out.writeVLong(updated);
        out.writeVInt(indexResponses.length);
        for (IndexUpdateByQueryResponse response : indexResponses) {
            response.writeTo(out);
        }
        out.writeStringArray(mainFailures);
    }
}
