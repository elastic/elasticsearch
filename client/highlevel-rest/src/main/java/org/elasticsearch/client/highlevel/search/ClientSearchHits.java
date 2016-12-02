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

package org.elasticsearch.client.highlevel.search;

import org.elasticsearch.client.highlevel.XContentAccessor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ClientSearchHits implements SearchHits {

    private Map<String, Object> hitsSection;
    private XContentAccessor objectPath;
    private ClientSearchHit[] hitsArray;

    public ClientSearchHits(Map<String, Object> hitsSection) {
        this.hitsSection = hitsSection;
        this.objectPath = new XContentAccessor(this.hitsSection);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field("hits", hitsSection);
    }

    @Override
    public Iterator<SearchHit> iterator() {
        return Arrays.asList(hits()).iterator();
    }

    @Override
    public long totalHits() {
        return this.objectPath.evaluateLong("total");
    }

    @Override
    public long getTotalHits() {
        return totalHits();
    }

    @Override
    public float maxScore() {
        return this.objectPath.evaluateDouble("max_score").floatValue();
    }

    @Override
    public float getMaxScore() {
        return maxScore();
    }

    @Override
    @SuppressWarnings("unchecked")
    public SearchHit[] hits() {
        if (this.hitsArray == null) {
            List<Object> hits = (List<Object>) this.objectPath.evaluate("hits");
            this.hitsArray = new ClientSearchHit[hits.size()];
            int i = 0;
            for (Object hit : hits) {
                hitsArray[i] = new ClientSearchHit((Map<String, Object>) hit);
                i++;
            }
        }
        return this.hitsArray;
    }

    @Override
    public SearchHit getAt(int position) {
        return hits()[position];
    }

    @Override
    public SearchHit[] getHits() {
        return hits();
    }

}
