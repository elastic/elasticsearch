/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.scheduler;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class Scheduler extends AbstractDiffable<Scheduler> implements ToXContent {

    private static final ParseField CONFIG_FIELD = new ParseField("config");
    private static final ParseField STATUS_FIELD = new ParseField("status");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("schedulers");

    public static final ConstructingObjectParser<Scheduler, Void> PARSER = new ConstructingObjectParser<>("scheduler",
            a -> new Scheduler(((SchedulerConfig.Builder) a[0]).build(), (SchedulerStatus) a[1]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), SchedulerConfig.PARSER, CONFIG_FIELD);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), (p, c) -> SchedulerStatus.fromString(p.text()), STATUS_FIELD,
                ObjectParser.ValueType.STRING);
    }

    private final SchedulerConfig config;
    private final SchedulerStatus status;

    public Scheduler(SchedulerConfig config, SchedulerStatus status) {
        this.config = config;
        this.status = status;
    }

    public Scheduler(StreamInput in) throws IOException {
        this.config = new SchedulerConfig(in);
        this.status = SchedulerStatus.fromStream(in);
    }

    public String getId() {
        return config.getId();
    }

    public String getJobId() {
        return config.getJobId();
    }

    public SchedulerConfig getConfig() {
        return config;
    }

    public SchedulerStatus getStatus() {
        return status;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        config.writeTo(out);
        status.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CONFIG_FIELD.getPreferredName(), config);
        builder.field(STATUS_FIELD.getPreferredName(), status);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Scheduler that = (Scheduler) o;
        return Objects.equals(config, that.config) &&
                Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config, status);
    }
}
