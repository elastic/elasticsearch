/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Returns the results for a {@link RankEvalRequest}.<br>
 * The response contains a detailed section for each evaluation query in the request and
 * possible failures that happened when execution individual queries.
 **/
public class RankEvalResponse extends ActionResponse implements ToXContentObject {

    /** The overall evaluation result. */
    private double metricScore;
    /** details about individual ranking evaluation queries, keyed by their id */
    private Map<String, EvalQueryQuality> details;
    /** exceptions for specific ranking evaluation queries, keyed by their id */
    private Map<String, Exception> failures;

    public RankEvalResponse(double metricScore, Map<String, EvalQueryQuality> partialResults,
            Map<String, Exception> failures) {
        this.metricScore = metricScore;
        this.details =  new HashMap<>(partialResults);
        this.failures = new HashMap<>(failures);
    }

    RankEvalResponse(StreamInput in) throws IOException {
        super(in);
        this.metricScore = in.readDouble();
        int partialResultSize = in.readVInt();
        this.details = new HashMap<>(partialResultSize);
        for (int i = 0; i < partialResultSize; i++) {
            String queryId = in.readString();
            EvalQueryQuality partial = new EvalQueryQuality(in);
            this.details.put(queryId, partial);
        }
        int failuresSize = in.readVInt();
        this.failures = new HashMap<>(failuresSize);
        for (int i = 0; i < failuresSize; i++) {
            String queryId = in.readString();
            this.failures.put(queryId, in.readException());
        }
    }

    public double getMetricScore() {
        return metricScore;
    }

    public Map<String, EvalQueryQuality> getPartialResults() {
        return Collections.unmodifiableMap(details);
    }

    public Map<String, Exception> getFailures() {
        return Collections.unmodifiableMap(failures);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(metricScore);
        out.writeVInt(details.size());
        for (String queryId : details.keySet()) {
            out.writeString(queryId);
            details.get(queryId).writeTo(out);
        }
        out.writeVInt(failures.size());
        for (String queryId : failures.keySet()) {
            out.writeString(queryId);
            out.writeException(failures.get(queryId));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("metric_score", metricScore);
        builder.startObject("details");
        for (String key : details.keySet()) {
            details.get(key).toXContent(builder, params);
        }
        builder.endObject();
        builder.startObject("failures");
        for (String key : failures.keySet()) {
            builder.startObject(key);
            ElasticsearchException.generateFailureXContent(builder, params, failures.get(key), true);
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    private static final ParseField DETAILS_FIELD = new ParseField("details");
    private static final ParseField FAILURES_FIELD = new ParseField("failures");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RankEvalResponse, Void> PARSER = new ConstructingObjectParser<>("rank_eval_response",
            true,
            a -> new RankEvalResponse((Double) a[0],
                    ((List<EvalQueryQuality>) a[1]).stream().collect(Collectors.toMap(EvalQueryQuality::getId, Function.identity())),
                    ((List<Tuple<String, Exception>>) a[2]).stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2))));
    static {
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), EvalQueryQuality.METRIC_SCORE_FIELD);
        PARSER.declareNamedObjects(ConstructingObjectParser.optionalConstructorArg(), (p, c, n) -> EvalQueryQuality.fromXContent(p, n),
                DETAILS_FIELD);
        PARSER.declareNamedObjects(ConstructingObjectParser.optionalConstructorArg(), (p, c, n) -> {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, p.nextToken(), p);
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, p.nextToken(), p);
            Tuple<String, ElasticsearchException> tuple = new Tuple<>(n, ElasticsearchException.failureFromXContent(p));
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, p.nextToken(), p);
            return tuple;
        }, FAILURES_FIELD);

    }

    public static RankEvalResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }
}
