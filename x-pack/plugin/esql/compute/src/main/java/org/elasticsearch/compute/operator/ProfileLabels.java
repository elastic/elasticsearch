/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Structured labels for the drivers and plans of a PROFILE response, so a client does not have to take apart the free text
 * {@code description}. They name where in the query a driver or plan ran:
 * <ul>
 *   <li>{@code kind}: what it is, the last part of the description. {@code data}, {@code node_reduce}, {@code final}, {@code merge}
 *       or {@code single}.</li>
 *   <li>{@code branch}: for a {@code FORK} or {@code UNION}, the path of subplans down to it, for example
 *       {@code subplan-0.subplan-1}, or {@code main} for the plan that reads the merge. It is what comes before the kind in the
 *       description, and is absent when the description is just the kind.</li>
 *   <li>{@code phase}: when the query had to run in several phases, such as {@code IN} subqueries or {@code INLINE STATS}, the phase
 *       the driver or plan belongs to: {@code subplan-N}, in the order they ran, or {@code main}. Absent for a query with one phase.
 *       It is separate from {@code branch}, because both count subplans from zero and a branch can sit inside a phase.</li>
 *   <li>{@code phase_kind}: why the phase ran, such as {@code semi_join}, {@code inline_join} or {@code main}.</li>
 * </ul>
 */
final class ProfileLabels {
    private ProfileLabels() {}

    static String kind(String description) {
        int dot = description.lastIndexOf('.');
        return dot < 0 ? description : description.substring(dot + 1);
    }

    @Nullable
    static String branch(String description) {
        int dot = description.lastIndexOf('.');
        return dot < 0 ? null : description.substring(0, dot);
    }

    /** Writes the labels as fields of the object being built. Nothing is written for a description the labels cannot describe. */
    static void write(XContentBuilder builder, String description, @Nullable String phase, @Nullable String phaseKind) throws IOException {
        if (description.isEmpty() == false) {
            builder.field("kind", kind(description));
            String branch = branch(description);
            if (branch != null) {
                builder.field("branch", branch);
            }
        }
        if (phase != null) {
            builder.field("phase", phase);
        }
        if (phaseKind != null) {
            builder.field("phase_kind", phaseKind);
        }
    }
}
