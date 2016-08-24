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

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * This class defines a ranking evaluation task including an id, a collection of queries to evaluate and the evaluation metric.
 *
 * Each QA run is based on a set of queries to send to the index and multiple QA specifications that define how to translate the query
 * intents into elastic search queries.
 * */

public class RankEvalSpec extends ToXContentToBytes implements Writeable {

    /** Collection of query specifications, that is e.g. search request templates to use for query translation. */
    private Collection<QuerySpec> specifications = new ArrayList<>();
    /** Definition of the quality metric, e.g. precision at N */
    private RankedListQualityMetric eval;
    /** a unique id for the whole QA task */
    private String specId;

    public RankEvalSpec() {
        // TODO think if no args ctor is okay
    }

    public RankEvalSpec(String specId, Collection<QuerySpec> specs, RankedListQualityMetric metric) {
        this.specId = specId;
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
        specId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(specifications.size());
        for (QuerySpec spec : specifications) {
            spec.writeTo(out);
        }
        out.writeNamedWriteable(eval);
        out.writeString(specId);
    }

    public void setEval(RankedListQualityMetric eval) {
        this.eval = eval;
    }

    public void setTaskId(String taskId) {
        this.specId = taskId;
    }

    public String getTaskId() {
        return this.specId;
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

    private static final ParseField SPECID_FIELD = new ParseField("spec_id");
    private static final ParseField METRIC_FIELD = new ParseField("metric");
    private static final ParseField REQUESTS_FIELD = new ParseField("requests");
    private static final ObjectParser<RankEvalSpec, RankEvalContext> PARSER = new ObjectParser<>("rank_eval", RankEvalSpec::new);

    static {
        PARSER.declareString(RankEvalSpec::setTaskId, SPECID_FIELD);
        PARSER.declareObject(RankEvalSpec::setEvaluator, (p, c) -> {
            try {
                return RankedListQualityMetric.fromXContent(p, c);
            } catch (IOException ex) {
                throw new ParsingException(p.getTokenLocation(), "error parsing rank request", ex);
            }
        } , METRIC_FIELD);
        PARSER.declareObjectArray(RankEvalSpec::setSpecifications, (p, c) -> {
            try {
                return QuerySpec.fromXContent(p, c);
            } catch (IOException ex) {
                throw new ParsingException(p.getTokenLocation(), "error parsing rank request", ex);
            }
        } , REQUESTS_FIELD);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SPECID_FIELD.getPreferredName(), this.specId);
        builder.startArray(REQUESTS_FIELD.getPreferredName());
        for (QuerySpec spec : this.specifications) {
            spec.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(METRIC_FIELD.getPreferredName(), this.eval);
        builder.endObject();
        return builder;
    }

    public static RankEvalSpec parse(XContentParser parser, RankEvalContext context) throws IOException {
        return PARSER.parse(parser, context);
    }

}
