/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.policy;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecider;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class AutoscalingPolicy extends AbstractDiffable<AutoscalingPolicy> implements Diffable<AutoscalingPolicy>, ToXContentObject {

    public static final String NAME = "autoscaling_policy";

    public static final ParseField DECIDERS_FIELD = new ParseField("deciders");

    public static final ConstructingObjectParser<AutoscalingPolicy, String> PARSER;

    static {
        PARSER = new ConstructingObjectParser<>(NAME, false, (c, name) -> {
            @SuppressWarnings("unchecked")
            final List<AutoscalingDecider> deciders = (List<AutoscalingDecider>) c[0];
            return new AutoscalingPolicy(name, deciders);
        });
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> p.namedObject(AutoscalingDecider.class, n, null),
            DECIDERS_FIELD
        );
    }

    public static AutoscalingPolicy parse(final XContentParser parser, final String name) {
        return PARSER.apply(parser, name);
    }

    private final String name;

    public String name() {
        return name;
    }

    private final List<AutoscalingDecider> deciders;

    public List<AutoscalingDecider> deciders() {
        return deciders;
    }

    public AutoscalingPolicy(final String name, final List<AutoscalingDecider> deciders) {
        this.name = Objects.requireNonNull(name);
        this.deciders = Objects.requireNonNull(deciders);
    }

    public AutoscalingPolicy(final StreamInput in) throws IOException {
        name = in.readString();
        deciders = in.readNamedWriteableList(AutoscalingDecider.class);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeNamedWriteableList(deciders);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(DECIDERS_FIELD.getPreferredName());
            {
                for (final AutoscalingDecider decider : deciders) {
                    builder.field(decider.name(), decider);
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final AutoscalingPolicy that = (AutoscalingPolicy) o;
        return name.equals(that.name) && deciders.equals(that.deciders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, deciders);
    }

}
