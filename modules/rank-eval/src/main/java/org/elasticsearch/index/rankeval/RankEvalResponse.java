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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
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
 * TODO get rid of either this or RankEvalResult
 **/
public class RankEvalResponse extends ActionResponse implements ToXContent {

    private RankEvalResult qualityResult;

    public RankEvalResponse() {

    }

    public RankEvalResponse(StreamInput in) throws IOException {
        super.readFrom(in);
        this.qualityResult = new RankEvalResult(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        qualityResult.writeTo(out);
    }

    public void setRankEvalResult(RankEvalResult result) {
        this.qualityResult = result;
    }

    public RankEvalResult getRankEvalResult() {
        return qualityResult;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("rank_eval");
        builder.field("spec_id", qualityResult.getSpecId());
        builder.field("quality_level", qualityResult.getQualityLevel());
        builder.startArray("unknown_docs");
        Map<String, Collection<RatedDocumentKey>> unknownDocs = qualityResult.getUnknownDocs();
        for (String key : unknownDocs.keySet()) {
            builder.startObject();
            builder.field(key, unknownDocs.get(key));
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

}
