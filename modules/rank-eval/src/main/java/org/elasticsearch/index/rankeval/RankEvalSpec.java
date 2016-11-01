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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Map;

/**
 * This class defines a ranking evaluation task including an id, a collection of queries to evaluate and the evaluation metric.
 *
 * Each QA run is based on a set of queries to send to the index and multiple QA specifications that define how to translate the query
 * intents into elastic search queries.
 * */

public class RankEvalSpec extends ToXContentToBytes implements Writeable {
    /** Collection of query specifications, that is e.g. search request templates to use for query translation. */
    private Collection<RatedRequest> ratedRequests = new ArrayList<>();
    /** Definition of the quality metric, e.g. precision at N */
    private RankedListQualityMetric metric;
    /** optional: Template to base test requests on */
    @Nullable
    private Script template;

    public RankEvalSpec() {
        // TODO think if no args ctor is okay
    }

    public RankEvalSpec(Collection<RatedRequest> specs, RankedListQualityMetric metric) {
        this.ratedRequests = specs;
        this.metric = metric;
    }

    public RankEvalSpec(StreamInput in) throws IOException {
        int specSize = in.readInt();
        ratedRequests = new ArrayList<>(specSize);
        for (int i = 0; i < specSize; i++) {
            ratedRequests.add(new RatedRequest(in));
        }
        metric = in.readNamedWriteable(RankedListQualityMetric.class);
        if (in.readBoolean()) {
            template = new Script(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(ratedRequests.size());
        for (RatedRequest spec : ratedRequests) {
            spec.writeTo(out);
        }
        out.writeNamedWriteable(metric);
        if (template != null) {
            out.writeBoolean(true);
            template.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    /** Set the metric to use for quality evaluation. */
    public void setMetric(RankedListQualityMetric metric) {
        this.metric = metric;
    }

    /** Returns the metric to use for quality evaluation.*/
    public RankedListQualityMetric getMetric() {
        return metric;
    }

    /** Returns a list of intent to query translation specifications to evaluate. */
    public Collection<RatedRequest> getSpecifications() {
        return ratedRequests;
    }

    /** Set the list of intent to query translation specifications to evaluate. */
    public void setSpecifications(Collection<RatedRequest> specifications) {
        this.ratedRequests = specifications;
    }
    
    /** Set the template to base test requests on. */
    public void setTemplate(Script script) {
        this.template = script;
    }
    
    /** Returns the template to base test requests on. */
    public Script getTemplate() {
        return this.template;
    }

    private static final ParseField TEMPLATE_FIELD = new ParseField("template");
    private static final ParseField METRIC_FIELD = new ParseField("metric");
    private static final ParseField REQUESTS_FIELD = new ParseField("requests");
    private static final ObjectParser<RankEvalSpec, RankEvalContext> PARSER = new ObjectParser<>("rank_eval", RankEvalSpec::new);

    static {
        PARSER.declareObject(RankEvalSpec::setMetric, (p, c) -> {
            try {
                return RankedListQualityMetric.fromXContent(p, c);
            } catch (IOException ex) {
                throw new ParsingException(p.getTokenLocation(), "error parsing rank request", ex);
            }
        } , METRIC_FIELD);
        PARSER.declareObject(RankEvalSpec::setTemplate, (p, c) -> {
                return Script.parse(p, c.getParseFieldMatcher(), "mustache");
        }, TEMPLATE_FIELD);
        PARSER.declareObjectArray(RankEvalSpec::setSpecifications, (p, c) -> {
            try {
                return RatedRequest.fromXContent(p, c);
            } catch (IOException ex) {
                throw new ParsingException(p.getTokenLocation(), "error parsing rank request", ex);
            }
        } , REQUESTS_FIELD);
    }

    public static RankEvalSpec parse(XContentParser parser, RankEvalContext context, boolean templated) throws IOException {
        RankEvalSpec spec = PARSER.parse(parser, context);

        if (templated) {
            for (RatedRequest query_spec : spec.getSpecifications()) {
                Map<String, String> params = query_spec.getParams();
                Script scriptWithParams = new Script(spec.template.getScript(), spec.template.getType(), spec.template.getLang(), params);
                String resolvedRequest = 
                        ((BytesReference) 
                                (context.getScriptService().executable(scriptWithParams, ScriptContext.Standard.SEARCH, params)
                                        .run()))
                        .utf8ToString();
                try (XContentParser subParser = XContentFactory.xContent(resolvedRequest).createParser(resolvedRequest)) {
                    QueryParseContext parseContext =
                            new QueryParseContext(
                                    context.getSearchRequestParsers().queryParsers, 
                                    subParser, 
                                    context.getParseFieldMatcher());
                    SearchSourceBuilder templateResult = 
                            SearchSourceBuilder.fromXContent(
                                    parseContext,
                                    context.getAggs(),
                                    context.getSuggesters(),
                                    context.getSearchExtParsers());
                    query_spec.setTestRequest(templateResult);
                }
            }
        }
        return spec; 
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (this.template != null) {
            builder.field(TEMPLATE_FIELD.getPreferredName(), this.template);
        }
        builder.startArray(REQUESTS_FIELD.getPreferredName());
        for (RatedRequest spec : this.ratedRequests) {
            spec.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(METRIC_FIELD.getPreferredName(), this.metric);
        builder.endObject();
        return builder;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RankEvalSpec other = (RankEvalSpec) obj;

        return Objects.equals(ratedRequests, other.ratedRequests) &&
                Objects.equals(metric, other.metric) &&
                Objects.equals(template, other.template);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(ratedRequests, metric, template);
    }
}
