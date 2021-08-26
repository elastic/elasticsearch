/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Specification of the ranking evaluation request.<br>
 * This class groups the queries to evaluate, including their document ratings,
 * and the evaluation metric including its parameters.
 */
public class RankEvalSpec implements Writeable, ToXContentObject {
    /** List of search request to use for the evaluation */
    private final List<RatedRequest> ratedRequests;
    /** Definition of the quality metric, e.g. precision at N */
    private final EvaluationMetric metric;
    /** Maximum number of requests to execute in parallel. */
    private int maxConcurrentSearches = MAX_CONCURRENT_SEARCHES;
    /** Default max number of requests. */
    private static final int MAX_CONCURRENT_SEARCHES = 10;
    /** optional: Templates to base test requests on */
    private final Map<String, Script> templates = new HashMap<>();

    public RankEvalSpec(List<RatedRequest> ratedRequests, EvaluationMetric metric, Collection<ScriptWithId> templates) {
        this.metric = Objects.requireNonNull(metric, "Cannot evaluate ranking if no evaluation metric is provided.");
        if (ratedRequests == null || ratedRequests.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot evaluate ranking if no search requests with rated results are provided. Seen: " + ratedRequests);
        }
        this.ratedRequests = ratedRequests;
        if (templates == null || templates.isEmpty()) {
            for (RatedRequest request : ratedRequests) {
                if (request.getEvaluationRequest() == null) {
                    throw new IllegalStateException("Cannot evaluate ranking if neither template nor evaluation request is "
                            + "provided. Seen for request id: " + request.getId());
                }
            }
        }
        if (templates != null) {
            for (ScriptWithId idScript : templates) {
                this.templates.put(idScript.id, idScript.script);
            }
        }
    }

    public RankEvalSpec(List<RatedRequest> ratedRequests, EvaluationMetric metric) {
        this(ratedRequests, metric, null);
    }

    public RankEvalSpec(StreamInput in) throws IOException {
        int specSize = in.readVInt();
        ratedRequests = new ArrayList<>(specSize);
        for (int i = 0; i < specSize; i++) {
            ratedRequests.add(new RatedRequest(in));
        }
        metric = in.readNamedWriteable(EvaluationMetric.class);
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
    public EvaluationMetric getMetric() {
        return metric;
    }

    /** Returns a list of intent to query translation specifications to evaluate. */
    public List<RatedRequest> getRatedRequests() {
        return Collections.unmodifiableList(ratedRequests);
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
    private static final ConstructingObjectParser<RankEvalSpec, Void> PARSER = new ConstructingObjectParser<>("rank_eval",
            a -> new RankEvalSpec((List<RatedRequest>) a[0], (EvaluationMetric) a[1], (Collection<ScriptWithId>) a[2]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> RatedRequest.fromXContent(p), REQUESTS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> parseMetric(p), METRIC_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> ScriptWithId.fromXContent(p),
                TEMPLATES_FIELD);
        PARSER.declareInt(RankEvalSpec::setMaxConcurrentSearches, MAX_CONCURRENT_SEARCHES_FIELD);
    }

    private static EvaluationMetric parseMetric(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        EvaluationMetric metric = parser.namedObject(EvaluationMetric.class, parser.currentName(), null);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return metric;
    }

    public static RankEvalSpec parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    static class ScriptWithId {
        private Script script;
        private String id;

        private static final ParseField TEMPLATE_FIELD = new ParseField("template");
        private static final ParseField TEMPLATE_ID_FIELD = new ParseField("id");

        ScriptWithId(String id, Script script) {
            this.id = id;
            this.script = script;
        }

        private static final ConstructingObjectParser<ScriptWithId, Void> PARSER =
                new ConstructingObjectParser<>("script_with_id",
                        a -> new ScriptWithId((String) a[0], (Script) a[1]));

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
    public String toString() {
        return Strings.toString(this);
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
