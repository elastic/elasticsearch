/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;

import java.io.IOException;
import java.util.Objects;

public class ScheduledEventToRuleWriter implements ToXContentObject {

    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField RULES = new ParseField("rules");

    private final String description;
    private final DetectionRule detectionRule;

    public ScheduledEventToRuleWriter(String description, DetectionRule detectionRule) {
        this.description = Objects.requireNonNull(description);
        this.detectionRule = Objects.requireNonNull(detectionRule);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DESCRIPTION.getPreferredName(), description);
        builder.array(RULES.getPreferredName(), detectionRule);
        builder.endObject();
        return builder;
    }
}
