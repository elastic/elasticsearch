/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Represents set of {@link LifecycleAction}s which should be executed at a
 * particular point in the lifecycle of an index.
 */
public class Phase implements ToXContentObject, Writeable {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(Phase.class);

    public static final ParseField MIN_AGE = new ParseField("min_age");
    public static final ParseField ACTIONS_FIELD = new ParseField("actions");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Phase, String> PARSER = new ConstructingObjectParser<>("phase", false, (a, name) -> {
        final List<LifecycleAction> lifecycleActions = (List<LifecycleAction>) a[1];
        Map<String, LifecycleAction> map = Maps.newMapWithExpectedSize(lifecycleActions.size());
        for (LifecycleAction lifecycleAction : lifecycleActions) {
            if (map.put(lifecycleAction.getWriteableName(), lifecycleAction) != null) {
                throw new IllegalStateException("Duplicate key");
            }
        }
        return new Phase(name, (TimeValue) a[0], map);
    });
    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (ContextParser<String, Object>) (p, c) -> TimeValue.parseTimeValue(p.text(), MIN_AGE.getPreferredName()),
            MIN_AGE,
            ValueType.VALUE
        );
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> p.namedObject(LifecycleAction.class, n, null),
            v -> {
                throw new IllegalArgumentException("ordered " + ACTIONS_FIELD.getPreferredName() + " are not supported");
            },
            ACTIONS_FIELD
        );
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
        builder.xContentValuesMap(ACTIONS_FIELD.getPreferredName(), actions);
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
        return Objects.equals(name, other.name) && Objects.equals(minimumAge, other.minimumAge) && Objects.equals(actions, other.actions);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    public boolean maybeAddDeprecationWarningForFreezeAction(String policyName) {
        if (getActions().containsKey(FreezeAction.NAME)) {
            deprecationLogger.warn(
                DeprecationCategory.OTHER,
                "ilm_freeze_action_deprecation",
                "The freeze action in ILM is deprecated and will be removed in a future version;"
                    + " this action is already a noop so it can be safely removed. Please remove the freeze action from the '"
                    + policyName
                    + "' policy."
            );
            return true;
        }
        return false;
    }

}
