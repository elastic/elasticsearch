/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.security.InternalClient;

import java.io.IOException;
import java.util.List;

public class LifecyclePolicy extends AbstractDiffable<LifecyclePolicy> implements ToXContentObject, Writeable {

    public static final ParseField PHASES_FIELD = new ParseField("phases");

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<LifecyclePolicy, Tuple<String, NamedXContentRegistry>> PARSER = new ConstructingObjectParser<>(
            "lifecycle_policy", false, (a, c) -> new LifecyclePolicy(c.v1(), (List<Phase>) a[0]));

    static {
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> Phase.parse(p, new Tuple<>(n, c.v2())),
                v -> {
                    throw new IllegalArgumentException("ordered " + PHASES_FIELD.getPreferredName() + " are not supported");
                }, PHASES_FIELD);
    }

    public static LifecyclePolicy parse(XContentParser parser, Tuple<String, NamedXContentRegistry> context) {
        return PARSER.apply(parser, context);
    }

    private String name;
    private List<Phase> phases;

    public LifecyclePolicy(String name, List<Phase> phases) {
        this.name = name;
        this.phases = phases;
    }

    public LifecyclePolicy(StreamInput in) throws IOException {
        name = in.readString();
        phases = in.readList(Phase::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeList(phases);
    }

    public String getName() {
        return name;
    }

    public List<Phase> getPhases() {
        return phases;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(PHASES_FIELD.getPreferredName(), phases);
        builder.startObject(PHASES_FIELD.getPreferredName());
        for (Phase phase : phases) {
            builder.field(phase.getName(), phase);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public void execute(IndexMetaData idxMeta, InternalClient client) {
    }
}
