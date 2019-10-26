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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Request to perform a search ranking evaluation.
 */
public class RankEvalRequest extends ActionRequest implements IndicesRequest.Replaceable {

    private RankEvalSpec rankingEvaluationSpec;

    private IndicesOptions indicesOptions  = SearchRequest.DEFAULT_INDICES_OPTIONS;
    private String[] indices = Strings.EMPTY_ARRAY;

    public RankEvalRequest(RankEvalSpec rankingEvaluationSpec, String[] indices) {
        this.rankingEvaluationSpec = Objects.requireNonNull(rankingEvaluationSpec, "ranking evaluation specification must not be null");
        indices(indices);
    }

    RankEvalRequest(StreamInput in) throws IOException {
        super(in);
        rankingEvaluationSpec = new RankEvalSpec(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
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
     * Set the specification of the ranking evaluation.
     */
    public void setRankEvalSpec(RankEvalSpec task) {
        this.rankingEvaluationSpec = task;
    }

    /**
     * Sets the indices the search will be executed on.
     */
    @Override
    public RankEvalRequest indices(String... indices) {
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
    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public void indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indicesOptions must not be null");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        rankingEvaluationSpec.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RankEvalRequest that = (RankEvalRequest) o;
        return Objects.equals(indicesOptions, that.indicesOptions) &&
                Arrays.equals(indices, that.indices) &&
                Objects.equals(rankingEvaluationSpec, that.rankingEvaluationSpec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indicesOptions, Arrays.hashCode(indices), rankingEvaluationSpec);
    }
}
