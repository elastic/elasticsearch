/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class LabelSetSpec {
    /** Labels known to be visible so far. */
    private final List<Attribute> declaredLabels;

    /**
     * Labels removed by WITHOUT so far.
     */
    private final List<Attribute> excludedLabels;

    /**
     * Labels requested by the current BY shape.
     * This can be wider than declaredLabels because BY labels missing from visible output
     * must still be preserved in the final output as null-filled attrs.
     */
    private final List<Attribute> byDeclaredLabels;

    /**
     * Concrete labels to resolve against at apply() time.
     * Empty means "not bound yet"; apply() then falls back to declaredLabels.
     */
    private final List<Attribute> availableLabels;

    /**
     * Exclusions inherited from parent demand.
     * Used only by apply(), mainly for the innermost aggregate, to build TimeSeriesWithout.
     */
    private final List<Attribute> inheritedExcludedLabels;

    private LabelSetSpec(
        List<Attribute> declaredLabels,
        List<Attribute> excludedLabels,
        List<Attribute> byDeclaredLabels,
        List<Attribute> availableLabels,
        List<Attribute> inheritedExcludedLabels
    ) {
        this.declaredLabels = List.copyOf(declaredLabels);
        this.excludedLabels = List.copyOf(excludedLabels);
        this.byDeclaredLabels = List.copyOf(byDeclaredLabels);
        this.availableLabels = List.copyOf(availableLabels);
        this.inheritedExcludedLabels = List.copyOf(inheritedExcludedLabels);
    }

    /**
     * Build an exact spec from known labels.
     */
    static LabelSetSpec of(List<Attribute> labels) {
        return of(labels, List.of());
    }

    /**
     * Build an exact spec and carry inherited exclusions.
     * Used when BY must preserve a parent WITHOUT so the innermost aggregate can
     * emit TimeSeriesWithout with the full exclusion set.
     */
    static LabelSetSpec of(List<Attribute> declared, List<Attribute> excluded) {
        return new LabelSetSpec(declared, unionByFieldName(List.of(), excluded), declared, List.of(), List.of());
    }

    /**
     * Apply BY semantics.
     * Keep only labels visible from the input, but remember the full BY list so apply()
     * can report missing BY labels for later null synthesis.
     */
    static LabelSetSpec by(LabelSetSpec input, List<Attribute> labels) {
        return new LabelSetSpec(intersection(input.declared(), labels), List.of(), labels, List.of(), List.of());
    }

    /**
     * Apply WITHOUT semantics.
     * Drop visible labels now and accumulate exclusions for later TimeSeriesWithout synthesis.
     */
    static LabelSetSpec without(LabelSetSpec input, List<Attribute> excluded) {
        List<Attribute> accumulated = unionByFieldName(input.excluded(), excluded);
        List<Attribute> visible = difference(input.declared(), excluded);
        return new LabelSetSpec(visible, accumulated, visible, List.of(), List.of());
    }

    /**
     * Clamp declared labels to what is available.
     * Empty availability means `trust the input as-is`.
     */
    static LabelSetSpec intersectWithLabels(LabelSetSpec input, List<Attribute> available) {
        if (available.isEmpty()) {
            return input;
        }
        List<Attribute> visible = intersection(input.declared(), available);
        return new LabelSetSpec(visible, List.of(), visible, List.of(), List.of());
    }

    /**
     * Empty spec.
     */
    static LabelSetSpec none() {
        return new LabelSetSpec(List.of(), List.of(), List.of(), List.of(), List.of());
    }

    /**
     * Name-based set difference.
     */
    static List<Attribute> difference(List<Attribute> from, List<Attribute> toRemove) {
        Set<String> removeNames = new HashSet<>();
        for (Attribute attr : toRemove) {
            removeNames.add(fieldName(attr));
        }
        List<Attribute> result = new ArrayList<>();
        for (Attribute attr : from) {
            if (removeNames.contains(fieldName(attr)) == false) {
                result.add(attr);
            }
        }
        return result;
    }

    /**
     * Name-based set intersection.
     */
    static List<Attribute> intersection(List<Attribute> requested, List<Attribute> available) {
        Set<String> availableNames = new HashSet<>();
        for (Attribute attr : available) {
            availableNames.add(fieldName(attr));
        }
        List<Attribute> result = new ArrayList<>();
        for (Attribute attr : requested) {
            if (availableNames.contains(fieldName(attr))) {
                result.add(attr);
            }
        }
        return result;
    }

    /**
     * Resolve requested labels against visible output.
     * Match by identity first, then by field name.
     */
    static List<Attribute> resolveLabels(List<Attribute> requested, List<Attribute> visibleOutput) {
        List<Attribute> resolved = new ArrayList<>();
        for (Attribute attribute : requested) {
            if (visibleOutput.contains(attribute)) {
                resolved.add(attribute);
                continue;
            }
            Attribute byName = findAttributeByFieldName(visibleOutput, fieldName(attribute));
            if (byName != null) {
                resolved.add(byName);
            }
        }
        return resolved;
    }

    static Attribute findAttributeByFieldName(List<Attribute> attributes, String fieldNameToFind) {
        for (Attribute attribute : attributes) {
            if (fieldName(attribute).equals(fieldNameToFind)) {
                return attribute;
            }
        }
        return null;
    }

    /**
     * Canonical name used by label algebra.
     * FieldAttribute uses fieldName(); everything else falls back to name().
     */
    static String fieldName(Attribute attr) {
        if (attr instanceof FieldAttribute fieldAttr) {
            return fieldAttr.fieldName().string();
        }
        return attr.name();
    }

    /**
     * Name-based union preserving first occurrence order.
     */
    static List<Attribute> unionByFieldName(List<Attribute> first, List<Attribute> second) {
        List<Attribute> result = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        addUniqueByFieldName(result, seen, first);
        addUniqueByFieldName(result, seen, second);
        return result;
    }

    private static void addUniqueByFieldName(List<Attribute> result, Set<String> seen, List<Attribute> attrs) {
        for (Attribute attr : attrs) {
            if (seen.add(fieldName(attr))) {
                result.add(attr);
            }
        }
    }

    /**
     * Keep only dimension attrs, deduped by field name.
     * Only these can feed TimeSeriesWithout.
     */
    static List<Attribute> dimensionAttributes(List<Attribute> attrs) {
        List<Attribute> result = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (Attribute attr : attrs) {
            if (attr instanceof FieldAttribute fa && fa.isDimension() && seen.add(fieldName(attr))) {
                result.add(attr);
            }
        }
        return result;
    }

    /**
     * Bind this spec to visible child output.
     * Used by outer aggregation before apply().
     */
    public LabelSetSpec withIncluded(List<Attribute> includedLabels) {
        return new LabelSetSpec(declaredLabels, excludedLabels, byDeclaredLabels, includedLabels, List.of());
    }

    /**
     * Bind inherited exclusions and self-resolve against declaredLabels.
     * Used by the innermost aggregate, where no child aggregate output exists yet.
     */
    public LabelSetSpec withExcluded(List<Attribute> excludedLabels) {
        return new LabelSetSpec(declaredLabels, this.excludedLabels, byDeclaredLabels, declaredLabels, excludedLabels);
    }

    /**
     * Finalize the deferred spec into concrete aggregate shape.
     *
     * Result contract:
     * - includedGroupings: actual grouping keys
     * - matchedAttributes: visible output attrs that must survive but are not keys
     * - missingAttributes: BY labels absent from visible output; caller null-fills them
     * - excludedGroupings: concrete dimension exclusions for TimeSeriesWithout
     */
    public LabelSet apply() {
        List<Attribute> available = availableLabels.isEmpty() ? declaredLabels : availableLabels;

        Attribute ts = findAttributeByFieldName(available, MetadataAttribute.TIMESERIES);
        List<Attribute> resolved = resolveLabels(byDeclaredLabels, available);
        List<Attribute> missing = difference(byDeclaredLabels, resolved);
        List<Attribute> excludedDimensions = dimensionAttributes(unionByFieldName(excludedLabels, inheritedExcludedLabels));

        if (ts != null) {
            List<Attribute> attrs = new ArrayList<>();
            for (Attribute attr : resolved) {
                if (MetadataAttribute.isTimeSeriesAttributeName(attr.name()) == false) {
                    attrs.add(attr);
                }
            }
            return new LabelSet(List.of(ts), attrs, missing, excludedDimensions);
        }

        return new LabelSet(resolved, List.of(), missing, excludedDimensions);
    }

    /**
     * Labels currently known to exist.
     * This is the visible label domain before final apply().
     */
    public List<Attribute> declared() {
        return declaredLabels;
    }

    /**
     * Labels known to be excluded by WITHOUT.
     * Empty means the spec is exact.
     */
    public List<Attribute> excluded() {
        return excludedLabels;
    }

    /**
     * Concrete aggregate shape produced by apply().
     * Translator code consumes this directly.
     */
    public record LabelSet(
        /**
         * Concrete grouping keys for the aggregate.
         *
         * If _timeseries is present, it is the grouping key and concrete label attrs
         * move to matchedAttributes. Otherwise these are ordinary concrete label keys.
         */
        List<Attribute> includedGroupings,

        /**
         * Visible label attrs that belong in the output shape but are not grouping keys.
         *
         * Outer aggregation carries these through pack/unpack so they survive grouping
         * without becoming split keys.
         */
        List<Attribute> matchedAttributes,

        /**
         * BY labels requested by the spec but absent from visible output.
         *
         * Caller turns these into null aliases so PromQL BY preserves declared labels
         * in the final output even when they are not visible in the child.
         */
        List<Attribute> missingAttributes,

        /**
         * Concrete dimension exclusions accumulated from WITHOUT.
         *
         * Innermost aggregation feeds these into TimeSeriesWithout when timeseries
         * grouping is needed.
         */
        List<Attribute> excludedGroupings
    ) {}
}
