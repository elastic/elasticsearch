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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to perform a search ranking evaluation.
 */
public class RankEvalRequest extends ActionRequest {

    private RankEvalSpec rankingEvaluationSpec;
    private String[] indices = Strings.EMPTY_ARRAY;

    public RankEvalRequest(RankEvalSpec rankingEvaluationSpec, String[] indices) {
        this.rankingEvaluationSpec = rankingEvaluationSpec;
        setIndices(indices);
    }

    RankEvalRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = null;
        if (rankingEvaluationSpec == null) {
            e = new ActionRequestValidationException();
            e.addValidationError("missing ranking evaluation specification");
        }
        return e;
    }

    /**
     * Returns the specification of the ranking evaluation.
     */
    public RankEvalSpec getRankEvalSpec() {
        return rankingEvaluationSpec;
    }

    /**
     * Set the the specification of the ranking evaluation.
     */
    public void setRankEvalSpec(RankEvalSpec task) {
        this.rankingEvaluationSpec = task;
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public RankEvalRequest setIndices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (String index : indices) {
            Objects.requireNonNull(index, "index must not be null");
        }
        this.indices = indices;
        return this;
    }

    /**
     * @return the indices for this request
     */
    public String[] getIndices() {
        return indices;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        rankingEvaluationSpec = new RankEvalSpec(in);
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            indices = in.readStringArray();
        } else {
            // readStringArray uses readVInt for size, we used readInt in 6.2
            int indicesSize = in.readInt();
            String[] indices = new String[indicesSize];
            for (int i = 0; i < indicesSize; i++) {
                indices[i] = in.readString();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        rankingEvaluationSpec.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            out.writeStringArray(indices);
        } else {
            // writeStringArray uses writeVInt for size, we used writeInt in 6.2
            out.writeInt(indices.length);
            for (String index : indices) {
                out.writeString(index);
            }
        }
    }
}
