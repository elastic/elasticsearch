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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

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
    /** Maximum number of requests to execute in parallel. */
    private int maxConcurrentSearches = MAX_CONCURRENT_SEARCHES;
    /** Default max number of requests. */
    private static final int MAX_CONCURRENT_SEARCHES = 10;
    /** optional: Templates to base test requests on */
    private Map<String, Script> templates = new HashMap<>();

    public RankEvalSpec(Collection<RatedRequest> ratedRequests, RankedListQualityMetric metric, Collection<ScriptWithId> templates) {
        if (ratedRequests == null || ratedRequests.size() < 1) {
            throw new IllegalStateException(
                    "Cannot evaluate ranking if no search requests with rated results are provided. Seen: " + ratedRequests);
        }
        if (metric == null) {
            throw new IllegalStateException(
                    "Cannot evaluate ranking if no evaluation metric is provided.");
        }
        if (templates == null || templates.size() < 1) {
            for (RatedRequest request : ratedRequests) {
                if (request.getTestRequest() == null) {
                    throw new IllegalStateException(
                            "Cannot evaluate ranking if neither template nor test request is provided. Seen for request id: "
                    + request.getId());
                }
            }
        }
        this.ratedRequests = ratedRequests;
        this.metric = metric;
        if (templates != null) {
            for (ScriptWithId idScript : templates) {
                this.templates.put(idScript.id, idScript.script);
            }
        }
    }

    public RankEvalSpec(Collection<RatedRequest> ratedRequests, RankedListQualityMetric metric) {
        this(ratedRequests, metric, null);
    }

    public RankEvalSpec(StreamInput in) throws IOException {
        int specSize = in.readVInt();
        ratedRequests = new ArrayList<>(specSize);
        for (int i = 0; i < specSize; i++) {
            ratedRequests.add(new RatedRequest(in));
        }
        metric = in.readNamedWriteable(RankedListQualityMetric.class);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            Script value = new Script(in);
            this.templates.put(key, value);
        }
        maxConcurrentSearches = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(ratedRequests.size());
        for (RatedRequest spec : ratedRequests) {
            spec.writeTo(out);
        }
        out.writeNamedWriteable(metric);
        out.writeVInt(templates.size());
        for (Entry<String, Script> entry : templates.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
        out.writeVInt(maxConcurrentSearches);
    }

    /** Returns the metric to use for quality evaluation.*/
    public RankedListQualityMetric getMetric() {
        return metric;
    }

    /** Returns a list of intent to query translation specifications to evaluate. */
    public Collection<RatedRequest> getRatedRequests() {
        return ratedRequests;
    }

    /** Returns the template to base test requests on. */
    public Map<String, Script> getTemplates() {
        return this.templates;
    }

    /** Returns the max concurrent searches allowed. */
    public int getMaxConcurrentSearches() {
        return this.maxConcurrentSearches;
    }

    /** Set the max concurrent searches allowed. */
    public void setMaxConcurrentSearches(int maxConcurrentSearches) {
        this.maxConcurrentSearches = maxConcurrentSearches;
    }

    private static final ParseField TEMPLATES_FIELD = new ParseField("templates");
    private static final ParseField METRIC_FIELD = new ParseField("metric");
    private static final ParseField REQUESTS_FIELD = new ParseField("requests");
    private static final ParseField MAX_CONCURRENT_SEARCHES_FIELD = new ParseField("max_concurrent_searches");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RankEvalSpec, Void> PARSER =
            new ConstructingObjectParser<>("rank_eval",
            a -> new RankEvalSpec((Collection<RatedRequest>) a[0], (RankedListQualityMetric) a[1], (Collection<ScriptWithId>) a[2]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> {
                return RatedRequest.fromXContent(p);
        } , REQUESTS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            try {
                return RankedListQualityMetric.fromXContent(p);
            } catch (IOException ex) {
                throw new ParsingException(p.getTokenLocation(), "error parsing rank request", ex);
            }
        } , METRIC_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                return ScriptWithId.fromXContent(p);
        }, TEMPLATES_FIELD);
        PARSER.declareInt(RankEvalSpec::setMaxConcurrentSearches, MAX_CONCURRENT_SEARCHES_FIELD);
    }

    public static RankEvalSpec parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static class ScriptWithId {
        private Script script;
        private String id;

        private static final ParseField TEMPLATE_FIELD = new ParseField("template");
        private static final ParseField TEMPLATE_ID_FIELD = new ParseField("id");

        public ScriptWithId(String id, Script script) {
            this.id = id;
            this.script = script;
        }

        private static final ConstructingObjectParser<ScriptWithId, Void> PARSER =
                new ConstructingObjectParser<>("script_with_id", a -> new ScriptWithId((String) a[0], (Script) a[1]));

        public static ScriptWithId fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TEMPLATE_ID_FIELD);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
                try {
                    return Script.parse(p, "mustache");
                } catch (IOException ex) {
                    throw new ParsingException(p.getTokenLocation(), "error parsing rank request", ex);
                }
            }, TEMPLATE_FIELD);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(TEMPLATES_FIELD.getPreferredName());
        for (Entry<String, Script> entry : templates.entrySet()) {
            builder.startObject();
            builder.field(ScriptWithId.TEMPLATE_ID_FIELD.getPreferredName(), entry.getKey());
            builder.field(ScriptWithId.TEMPLATE_FIELD.getPreferredName(), entry.getValue());
            builder.endObject();
        }
        builder.endArray();

        builder.startArray(REQUESTS_FIELD.getPreferredName());
        for (RatedRequest spec : this.ratedRequests) {
            spec.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(METRIC_FIELD.getPreferredName(), this.metric);
        builder.field(MAX_CONCURRENT_SEARCHES_FIELD.getPreferredName(), maxConcurrentSearches);
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
                Objects.equals(maxConcurrentSearches, other.maxConcurrentSearches) &&
                Objects.equals(templates, other.templates);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(ratedRequests, metric, templates, maxConcurrentSearches);
    }
}
