/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
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

    private IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;
    private String[] indices = Strings.EMPTY_ARRAY;

    private SearchType searchType = SearchType.DEFAULT;

    public RankEvalRequest(RankEvalSpec rankingEvaluationSpec, String[] indices) {
        this.rankingEvaluationSpec = Objects.requireNonNull(rankingEvaluationSpec, "ranking evaluation specification must not be null");
        indices(indices);
    }

    RankEvalRequest(StreamInput in) throws IOException {
        super(in);
        rankingEvaluationSpec = new RankEvalSpec(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
            searchType = SearchType.fromId(in.readByte());
        }
    }

    RankEvalRequest() {}

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

    /**
     * The search type to execute, defaults to {@link SearchType#DEFAULT}.
     */
    public void searchType(SearchType searchType) {
        this.searchType = Objects.requireNonNull(searchType, "searchType must not be null");
    }

    /**
     * The type of search to execute.
     */
    public SearchType searchType() {
        return searchType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        rankingEvaluationSpec.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
            out.writeByte(searchType.id());
        }
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
        return Objects.equals(indicesOptions, that.indicesOptions)
            && Arrays.equals(indices, that.indices)
            && Objects.equals(rankingEvaluationSpec, that.rankingEvaluationSpec)
            && Objects.equals(searchType, that.searchType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indicesOptions, Arrays.hashCode(indices), rankingEvaluationSpec, searchType);
    }
}
