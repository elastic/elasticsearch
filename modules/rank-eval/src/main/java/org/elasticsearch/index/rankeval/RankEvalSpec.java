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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * This class defines a qa task including query intent and query spec.
 *
 * Each QA run is based on a set of queries to send to the index and multiple QA specifications that define how to translate the query
 * intents into elastic search queries. In addition it contains the quality metrics to compute.
 * */

public class RankEvalSpec implements Writeable {

    /** Collection of query specifications, that is e.g. search request templates to use for query translation. */
    private Collection<QuerySpec> specifications = new ArrayList<>();
    /** Definition of the quality metric, e.g. precision at N */
    private RankedListQualityMetric eval;
    /** a unique id for the whole QA task */
    private String taskId;

    public RankEvalSpec() {
        // TODO think if no args ctor is okay
    }

    public RankEvalSpec(String taskId, Collection<QuerySpec> specs, RankedListQualityMetric metric) {
        this.taskId = taskId;
        this.specifications = specs;
        this.eval = metric;
    }

    public RankEvalSpec(StreamInput in) throws IOException {
        int specSize = in.readInt();
        specifications = new ArrayList<>(specSize);
        for (int i = 0; i < specSize; i++) {
            specifications.add(new QuerySpec(in));
        }
        eval = in.readNamedWriteable(RankedListQualityMetric.class);
        taskId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(specifications.size());
        for (QuerySpec spec : specifications) {
            spec.writeTo(out);
        }
        out.writeNamedWriteable(eval);
        out.writeString(taskId);
    }

    public void setEval(RankedListQualityMetric eval) {
        this.eval = eval;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskId() {
        return this.taskId;
    }

    /** Returns the precision at n configuration (containing level of n to consider).*/
    public RankedListQualityMetric getEvaluator() {
        return eval;
    }

    /** Sets the precision at n configuration (containing level of n to consider).*/
    public void setEvaluator(RankedListQualityMetric config) {
        this.eval = config;
    }

    /** Returns a list of intent to query translation specifications to evaluate. */
    public Collection<QuerySpec> getSpecifications() {
        return specifications;
    }

    /** Set the list of intent to query translation specifications to evaluate. */
    public void setSpecifications(Collection<QuerySpec> specifications) {
        this.specifications = specifications;
    }

}
