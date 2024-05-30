/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.models;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class RiskInput implements ToXContentObject {
    private final String id;
    private final String index;
    private final String time;
    private final String ruleName;
    private final String category;
    private final double score;
    private final double contribution;

    public RiskInput(String id, String index, String time, String ruleName, String category, double score, double contribution) {
        this.id = id;
        this.index = index;
        this.time = time;
        this.ruleName = ruleName;
        this.category = category;
        this.score = score;
        this.contribution = contribution;
    }

    public static RiskInput fromAlertHit(SearchHit alert, double contribution) {
        var source = alert.getSourceAsMap();
        return new RiskInput(
            source.get("kibana.alert.uuid").toString(),
            alert.getIndex(),
            source.get("@timestamp").toString(),
            source.get("kibana.alert.rule.name").toString(),
            source.get("event.kind").toString(),
            Double.parseDouble(source.get("kibana.alert.risk_score").toString()),
            contribution
        );
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        builder.field("index", index);
        builder.field("time", time);
        builder.field("rule_name", ruleName);
        builder.field("category", category);
        builder.field("score", score);
        builder.field("contribution", contribution);
        builder.endObject();
        return builder;
    }
}
