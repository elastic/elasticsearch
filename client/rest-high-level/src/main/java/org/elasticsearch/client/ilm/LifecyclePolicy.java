/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the lifecycle of an index from creation to deletion. A
 * {@link LifecyclePolicy} is made up of a set of {@link Phase}s which it will
 * move through.
 */
public class LifecyclePolicy implements ToXContentObject {
    static final ParseField PHASES_FIELD = new ParseField("phases");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<LifecyclePolicy, String> PARSER = new ConstructingObjectParser<>(
        "lifecycle_policy",
        true,
        (a, name) -> {
            List<Phase> phases = (List<Phase>) a[0];
            Map<String, Phase> phaseMap = phases.stream().collect(Collectors.toMap(Phase::getName, Function.identity()));
            return new LifecyclePolicy(name, phaseMap);
        }
    );
    private static Map<String, Set<String>> ALLOWED_ACTIONS = new HashMap<>();

    static {
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> Phase.parse(p, n),
            v -> { throw new IllegalArgumentException("ordered " + PHASES_FIELD.getPreferredName() + " are not supported"); },
            PHASES_FIELD
        );

        ALLOWED_ACTIONS.put(
            "hot",
            Sets.newHashSet(
                UnfollowAction.NAME,
                SetPriorityAction.NAME,
                RolloverAction.NAME,
                ReadOnlyAction.NAME,
                ShrinkAction.NAME,
                ForceMergeAction.NAME,
                SearchableSnapshotAction.NAME
            )
        );
        ALLOWED_ACTIONS.put(
            "warm",
            Sets.newHashSet(
                UnfollowAction.NAME,
                SetPriorityAction.NAME,
                MigrateAction.NAME,
                AllocateAction.NAME,
                ForceMergeAction.NAME,
                ReadOnlyAction.NAME,
                ShrinkAction.NAME
            )
        );
        ALLOWED_ACTIONS.put(
            "cold",
            Sets.newHashSet(
                UnfollowAction.NAME,
                SetPriorityAction.NAME,
                MigrateAction.NAME,
                AllocateAction.NAME,
                FreezeAction.NAME,
                SearchableSnapshotAction.NAME,
                ReadOnlyAction.NAME
            )
        );
        ALLOWED_ACTIONS.put("frozen", Sets.newHashSet(UnfollowAction.NAME, SearchableSnapshotAction.NAME));
        ALLOWED_ACTIONS.put("delete", Sets.newHashSet(DeleteAction.NAME, WaitForSnapshotAction.NAME));
    }

    private final String name;
    private final Map<String, Phase> phases;

    /**
     * @param name
     *            the name of this {@link LifecyclePolicy}
     * @param phases
     *            a {@link Map} of {@link Phase}s which make up this
     *            {@link LifecyclePolicy}.
     */
    public LifecyclePolicy(String name, Map<String, Phase> phases) {
        phases.values().forEach(phase -> {
            if (ALLOWED_ACTIONS.containsKey(phase.getName()) == false) {
                throw new IllegalArgumentException("Lifecycle does not support phase [" + phase.getName() + "]");
            }
            if (phase.getName().equals("delete") && phase.getActions().size() == 0) {
                throw new IllegalArgumentException("phase [" + phase.getName() + "] must define actions");
            }
            phase.getActions().forEach((actionName, action) -> {
                if (ALLOWED_ACTIONS.get(phase.getName()).contains(actionName) == false) {
                    throw new IllegalArgumentException(
                        "invalid action [" + actionName + "] " + "defined in phase [" + phase.getName() + "]"
                    );
                }
            });
        });
        this.name = name;
        this.phases = phases;
    }

    public static LifecyclePolicy parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    /**
     * @return the name of this {@link LifecyclePolicy}
     */
    public String getName() {
        return name;
    }

    /**
     * @return the {@link Phase}s for this {@link LifecyclePolicy} in the order
     *         in which they will be executed.
     */
    public Map<String, Phase> getPhases() {
        return phases;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(PHASES_FIELD.getPreferredName());
        for (Phase phase : phases.values()) {
            builder.field(phase.getName(), phase);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, phases);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        LifecyclePolicy other = (LifecyclePolicy) obj;
        return Objects.equals(name, other.name) && Objects.equals(phases, other.phases);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
