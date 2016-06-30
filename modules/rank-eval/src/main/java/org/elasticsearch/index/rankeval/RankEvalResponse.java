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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/** 
 * For each qa specification identified by its id this response returns the respective
 * averaged precisionAnN value.
 * 
 * In addition for each query the document ids that haven't been found annotated is returned as well.
 * 
 * Documents of unknown quality - i.e. those that haven't been supplied in the set of annotated documents but have been returned
 * by the search are not taken into consideration when computing precision at n - they are ignored.
 * 
 **/
public class RankEvalResponse extends ActionResponse {

    private Collection<RankEvalResult> qualityResults = new ArrayList<>();

    public RankEvalResponse() {
        
    }

    public RankEvalResponse(StreamInput in) throws IOException {
        int size = in.readInt();
        qualityResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            qualityResults.add(new RankEvalResult(in));
        }
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(qualityResults.size());
        for (RankEvalResult result : qualityResults) {
            result.writeTo(out);
        }
    }    
 
    public void addRankEvalResult(int specId, double quality, Map<Integer, Collection<String>> unknownDocs) {
        RankEvalResult result = new RankEvalResult(specId, quality, unknownDocs);
        this.qualityResults.add(result);
    }
    
    public Collection<RankEvalResult> getRankEvalResults() {
        return qualityResults;
    }

}
