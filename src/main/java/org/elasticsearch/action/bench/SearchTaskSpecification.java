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

package org.elasticsearch.action.bench;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("rawtypes") 
public class SearchTaskSpecification implements Streamable {
    /** For each search request define how to evaluate whether it succeeded or not. */
    private Map<SearchRequest, Evaluator> specMap = new HashMap<SearchRequest, Evaluator>();
    
    public List<SearchRequest> getSearchRequests() {
        return Lists.newArrayList(specMap.keySet());
    }
    
    public Map<SearchRequest, Evaluator> getSearchTasks() {
        return specMap;
    }
    
    public void putSearchTask(SearchRequest request, Evaluator evaluator) {
        specMap.put(request, evaluator);
    }
    
    public void putAllSearchTasks(Map<SearchRequest, Evaluator> searchTasks) {
        specMap.putAll(searchTasks);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            SearchRequest request = new SearchRequest();
            request.readFrom(in);
            int evalID = in.readInt();
            Evaluator eval = BenchEvaluators.getNewObjectForId(evalID);
            eval.readFrom(in);
            specMap.put(request, eval);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(specMap.size());
        for (Entry<SearchRequest, Evaluator> entry : specMap.entrySet()) {
            entry.getKey().writeTo(out);
            out.writeInt(BenchEvaluators.getIdForObject(entry.getValue()));
            entry.getValue().writeTo(out);
        }
    }
    
    @Override
    public String toString() {
        ToStringHelper help = MoreObjects.toStringHelper(this);
        help.add("Number of requests", specMap.size());
        return help.toString();
    }
}
