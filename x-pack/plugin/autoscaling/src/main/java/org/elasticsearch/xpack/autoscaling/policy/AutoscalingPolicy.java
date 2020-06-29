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
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AutoscalingPolicy extends AbstractDiffable<AutoscalingPolicy> implements Diffable<AutoscalingPolicy>, ToXContentObject {

    public static final String NAME = "autoscaling_policy";

    public static final ParseField DECIDERS_FIELD = new ParseField("deciders");

    private static final ConstructingObjectParser<AutoscalingPolicy, String> PARSER;

    static {
        PARSER = new ConstructingObjectParser<>(NAME, false, (c, name) -> {
            @SuppressWarnings("unchecked")
            final List<Map.Entry<String, AutoscalingDecider>> deciders = (List<Map.Entry<String, AutoscalingDecider>>) c[0];
            return new AutoscalingPolicy(
                name,
                new TreeMap<>(deciders.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
            );
        });
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> new AbstractMap.SimpleEntry<>(n, p.namedObject(AutoscalingDecider.class, n, null)),
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

    private final SortedMap<String, AutoscalingDecider> deciders;

    public SortedMap<String, AutoscalingDecider> deciders() {
        return deciders;
    }

    public AutoscalingPolicy(final String name, final SortedMap<String, AutoscalingDecider> deciders) {
        this.name = Objects.requireNonNull(name);
        // TODO: validate that the policy deciders are non-empty
        this.deciders = Objects.requireNonNull(deciders);
    }

    public AutoscalingPolicy(final StreamInput in) throws IOException {
        name = in.readString();
        deciders = new TreeMap<>(
            in.readNamedWriteableList(AutoscalingDecider.class)
                .stream()
                .collect(Collectors.toMap(AutoscalingDecider::name, Function.identity()))
        );
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeNamedWriteableList(deciders.values().stream().collect(Collectors.toUnmodifiableList()));
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(DECIDERS_FIELD.getPreferredName());
            {
                for (final Map.Entry<String, AutoscalingDecider> entry : deciders.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
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
