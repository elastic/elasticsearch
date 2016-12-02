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

package org.elasticsearch.client;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SearchHits implements Iterable<SearchHit> {

    private final XContentAccessor objectPath;
    private SearchHit[] hitsArray;

    public SearchHits(Map<String, Object> hitsSection) {
        this.objectPath = new XContentAccessor(hitsSection);
    }

    @Override
    public Iterator<SearchHit> iterator() {
        return Arrays.asList(getHits()).iterator();
    }

    public long getTotalHits() {
        return this.objectPath.evaluateLong("total");
    }

    public float getMaxScore() {
        return this.objectPath.evaluateDouble("max_score").floatValue();
    }

    @SuppressWarnings("unchecked")
    public SearchHit[] getHits() {
        //TODO won't this cause concurrency issues?
        if (this.hitsArray == null) {
            List<Object> hits = (List<Object>) this.objectPath.evaluate("hits");
            this.hitsArray = new SearchHit[hits.size()];
            int i = 0;
            for (Object hit : hits) {
                hitsArray[i] = new SearchHit((Map<String, Object>) hit);
                i++;
            }
        }
        return this.hitsArray;
    }

    public SearchHit getAt(int position) {
        return getHits()[position];
    }
}
