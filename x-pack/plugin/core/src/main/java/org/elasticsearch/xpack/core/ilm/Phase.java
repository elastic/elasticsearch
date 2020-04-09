/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents set of {@link LifecycleAction}s which should be executed at a
 * particular point in the lifecycle of an index.
 */
public class Phase implements ToXContentObject, Writeable {

    public static final ParseField MIN_AGE = new ParseField("min_age");
    public static final ParseField ACTIONS_FIELD = new ParseField("actions");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Phase, String> PARSER = new ConstructingObjectParser<>("phase", false,
            (a, name) -> new Phase(name, (TimeValue) a[0], ((List<LifecycleAction>) a[1]).stream()
                    .collect(Collectors.toMap(LifecycleAction::getWriteableName, Function.identity()))));
    static {
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.text(), MIN_AGE.getPreferredName()), MIN_AGE, ValueType.VALUE);
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(),
                (p, c, n) -> p.namedObject(LifecycleAction.class, n, null), v -> {
                    throw new IllegalArgumentException("ordered " + ACTIONS_FIELD.getPreferredName() + " are not supported");
                }, ACTIONS_FIELD);
    }

    public static Phase parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    private final String name;
    private final Map<String, LifecycleAction> actions;
    private final TimeValue minimumAge;

    /**
     * @param name
     *            the name of this {@link Phase}.
     * @param minimumAge
     *            the age of the index when the index should move to this
     *            {@link Phase}.
     * @param actions
     *            a {@link Map} of the {@link LifecycleAction}s to run when
     *            during his {@link Phase}. The keys in this map are the associated
     *            action names. The order of these actions is defined
     *            by the {@link LifecycleType}
     */
    public Phase(String name, TimeValue minimumAge, Map<String, LifecycleAction> actions) {
        this.name = name;
        if (minimumAge == null) {
            this.minimumAge = TimeValue.ZERO;
        } else {
            this.minimumAge = minimumAge;
        }
        this.actions = actions;
    }

    /**
     * For Serialization
     */
    public Phase(StreamInput in) throws IOException {
        this.name = in.readString();
        this.minimumAge = in.readTimeValue();
        int size = in.readVInt();
        TreeMap<String, LifecycleAction> actions = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            actions.put(in.readString(), in.readNamedWriteable(LifecycleAction.class));
        }
        this.actions = actions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeTimeValue(minimumAge);
        out.writeVInt(actions.size());
        for (Map.Entry<String, LifecycleAction> entry : actions.entrySet()) {
            out.writeString(entry.getKey());
            out.writeNamedWriteable(entry.getValue());
        }
    }

    /**
     * @return the age of the index when the index should move to this
     *         {@link Phase}.
     */
    public TimeValue getMinimumAge() {
        return minimumAge;
    }

    /**
     * @return the name of this {@link Phase}
     */
    public String getName() {
        return name;
    }

    /**
     * @return a {@link Map} of the {@link LifecycleAction}s to run when during
     *         his {@link Phase}.
     */
    public Map<String, LifecycleAction> getActions() {
        return actions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MIN_AGE.getPreferredName(), minimumAge.getStringRep());
        builder.field(ACTIONS_FIELD.getPreferredName(), actions);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, minimumAge, actions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        Phase other = (Phase) obj;
        return Objects.equals(name, other.name) &&
                Objects.equals(minimumAge, other.minimumAge) &&
                Objects.equals(actions, other.actions);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

}
