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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class RiskInput implements ToXContentObject {
    private final String id;
    private final String index;
    private final String timestamp;
    private final String ruleName;
    private final String category;
    private final double riskScore;
    private final double contributionScore;

    /**
     * A risk input is an alert which contributed to the risk score for an entity
     * currently we return the top 10 risk inputs for a given entity
     * @param id
     * @param index
     * @param timestamp
     * @param ruleName
     * @param category
     * @param riskScore
     * @param contributionScore
     */
    public RiskInput(
        String id,
        String index,
        String timestamp,
        String ruleName,
        String category,
        double riskScore,
        double contributionScore
    ) {
        this.id = id;
        this.index = index;
        this.timestamp = timestamp;
        this.ruleName = ruleName;
        this.category = category;
        this.riskScore = riskScore;
        this.contributionScore = contributionScore;
    }

    public static RiskInput fromAlertHit(SearchHit alert, double contribution) {
        var source = alert.getSourceAsMap();

        long unixTimestamp = Long.parseLong(source.get("@timestamp").toString());
        Instant timestamp = Instant.ofEpochMilli(unixTimestamp);
        String formattedTimestamp = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC).format(timestamp);

        return new RiskInput(
            source.get("kibana.alert.uuid").toString(),
            alert.getIndex(),
            formattedTimestamp,
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
        builder.field("timestamp", timestamp);
        builder.field("rule_name", ruleName);
        builder.field("category", category);
        builder.field("risk_score", riskScore);
        builder.field("contribution_score", contributionScore);
        builder.endObject();
        return builder;
    }
}
