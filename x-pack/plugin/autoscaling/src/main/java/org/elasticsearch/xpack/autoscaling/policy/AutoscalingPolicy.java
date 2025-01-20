/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.policy;

import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

public class AutoscalingPolicy implements SimpleDiffable<AutoscalingPolicy>, ToXContentObject {

    public static final String NAME = "autoscaling_policy";

    public static final ParseField ROLES_FIELD = new ParseField("roles");
    public static final ParseField DECIDERS_FIELD = new ParseField("deciders");

    private static final ConstructingObjectParser<AutoscalingPolicy, String> PARSER;

    static {
        PARSER = new ConstructingObjectParser<>(NAME, false, (c, name) -> {
            @SuppressWarnings("unchecked")
            final List<String> roles = (List<String>) c[0];
            @SuppressWarnings("unchecked")
            final var deciders = (List<Map.Entry<String, Settings>>) c[1];
            return new AutoscalingPolicy(
                name,
                roles.stream().collect(Sets.toUnmodifiableSortedSet()),
                deciders.stream().collect(Maps.toUnmodifiableSortedMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        });
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), ROLES_FIELD);
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> {
            p.nextToken();
            return new AbstractMap.SimpleEntry<>(n, Settings.fromXContent(p));
        }, DECIDERS_FIELD);
    }

    public static AutoscalingPolicy parse(final XContentParser parser, final String name) {
        return PARSER.apply(parser, name);
    }

    private final String name;

    public String name() {
        return name;
    }

    private final SortedSet<String> roles;

    public SortedSet<String> roles() {
        return roles;
    }

    private final SortedMap<String, Settings> deciders;

    public SortedMap<String, Settings> deciders() {
        return deciders;
    }

    public AutoscalingPolicy(final String name, SortedSet<String> roles, final SortedMap<String, Settings> deciders) {
        this.name = Objects.requireNonNull(name);
        this.roles = Objects.requireNonNull(roles);
        this.deciders = Objects.requireNonNull(deciders);
    }

    public AutoscalingPolicy(final StreamInput in) throws IOException {
        this.name = in.readString();
        this.roles = in.readCollectionAsSet(StreamInput::readString).stream().collect(Sets.toUnmodifiableSortedSet());
        int deciderCount = in.readInt();
        SortedMap<String, Settings> decidersMap = new TreeMap<>();
        for (int i = 0; i < deciderCount; ++i) {
            decidersMap.put(in.readString(), Settings.readSettingsFromStream(in));
        }
        this.deciders = Collections.unmodifiableSortedMap(decidersMap);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringCollection(roles);
        out.writeInt(deciders.size());
        for (Map.Entry<String, Settings> entry : deciders.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.array(ROLES_FIELD.getPreferredName(), roles.toArray(String[]::new));
            builder.startObject(DECIDERS_FIELD.getPreferredName());
            {
                for (final Map.Entry<String, Settings> entry : deciders.entrySet()) {
                    builder.startObject(entry.getKey());
                    entry.getValue().toXContent(builder, params);
                    builder.endObject();
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
        return name.equals(that.name) && roles.equals(that.roles) && deciders.equals(that.deciders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, roles, deciders);
    }

}
