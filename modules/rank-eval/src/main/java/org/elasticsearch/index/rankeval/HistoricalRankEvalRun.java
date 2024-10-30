/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * @param runId  GUID */
public record HistoricalRankEvalRun(
    String runId,
    String storedCorpus,
    String templateId,
    org.elasticsearch.index.rankeval.HistoricalRankEvalRun.Metric metric,
    double metricScore,
    List<QueryResult> queryResults
) implements ToXContentObject {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("run_id", runId);
        builder.field("stored_corpus", storedCorpus);
        builder.field("template_id", templateId);
        builder.startObject("metric");
        builder.field("name", metric.name());
        builder.field("k", metric.k());
        builder.endObject();
        builder.field("metric_score", metricScore);
        builder.startArray("queries");
        for (QueryResult queryResult : queryResults) {
            builder.startObject();
            builder.field("query_id", queryResult.queryId());
            builder.field("metric_score", queryResult.metricScore());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    record QueryResult(String queryId, float metricScore) {}

    // This does not return metric-specific details such as relevantRatingThreshold, normalize, ignoreUnlabeled.
    // We would probably want to keep this in a non-POC.
    record Metric(String name, int k) {}
}
