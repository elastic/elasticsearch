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

public class ClientSearchHits implements Iterable<ClientSearchHit> {

    private final XContentAccessor objectPath;
    private ClientSearchHit[] hitsArray;

    public ClientSearchHits(Map<String, Object> hitsSection) {
        this.objectPath = new XContentAccessor(hitsSection);
    }

    @Override
    public Iterator<ClientSearchHit> iterator() {
        return Arrays.asList(getHits()).iterator();
    }

    public long getTotalHits() {
        return this.objectPath.evaluateLong("total");
    }

    public float getMaxScore() {
        return this.objectPath.evaluateDouble("max_score").floatValue();
    }

    @SuppressWarnings("unchecked")
    public ClientSearchHit[] getHits() {
        //TODO won't this cause concurrency issues?
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

    public ClientSearchHit getAt(int position) {
        return getHits()[position];
    }
}
