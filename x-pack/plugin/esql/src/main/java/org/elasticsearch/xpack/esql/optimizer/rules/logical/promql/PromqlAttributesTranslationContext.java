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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The label-grouping fragment of the PromQL -> ESQL syntax-directed translation.
 * <p>
 * A PromQL query such as {@code sum by(cluster) (avg without(region) (cpu_util))} is a chain of aggregates over a single
 * leaf selector. PromQL grouping is <em>dynamic</em>: the concrete set of label names lives on disk and is unknown at
 * plan time, so we cannot list it efficiently. Instead, the translation carries the aggregation shape symbolically
 * and resolves it against real columns only at the very end.
 *
 * <h2>Abstract domain: label sets</h2>
 * A label set is a plain {@code List<Attribute>}, with the single sentinel {@code ALL} standing for the full universe
 * {@code T} ("every runtime label") - the one set we can never enumerate at plan time. {@code T} is realised physically
 * by the opaque {@code _timeseries} grouping key (see {@code TranslateTimeSeriesWithout}), which is why it contributes
 * no concrete columns when resolved. The static {@code union}/{@code intersect}/{@code minus} helpers implement set
 * algebra over these values (labels are compared by field name). {@code T} minus a finite set stays {@code T}: the
 * removed labels are never listed, and - because every {@code BY} forces a finite scope down its subtree - that
 * complement is never observed, so a single {@code ALL} sentinel is enough.
 *
 * <h2>The two attributes</h2>
 * The translation is an <em>L-attributed</em> syntax-directed definition. Two attributes flow through the aggregate
 * tree, modelled here as distinct immutable value types:
 * <ul>
 *   <li>{@link InheritedAttributes} - the <b>inherited</b> attribute, threaded <em>down</em> the chain. It answers "what
 *   must the subtree below preserve?". Each aggregate narrows it ({@link InheritedAttributes#including}/
 *   {@link InheritedAttributes#excluding}); the leaf selector turns it into the shape it exposes
 *   ({@link InheritedAttributes#reflect}).</li>
 *   <li>{@link SynthesizedAttributes} - the <b>synthesized</b> attribute, folded <em>up</em> the chain. It answers "what
 *   does the subtree expose?". Each aggregate folds its grouping over its child's shape
 *   ({@link SynthesizedAttributes#foldIncluding}/{@link SynthesizedAttributes#foldExcluding}).</li>
 * </ul>
 * Two passes are unavoidable: this is an L-attributed (not S-attributed) definition, and the translation can only
 * resolve symbolic labels against the <em>concrete</em> columns a child subtree actually produces, which exist only
 * after the child is translated.
 *
 * <h2>The translation: from attribute to target representation</h2>
 * A {@link SynthesizedAttributes} is symbolic; it cannot be handed to a plan node directly. {@link SynthesizedAttributes#translate}/
 * {@link SynthesizedAttributes#translateLeaf} are the <b>code-generation semantic action</b> that concretizes it against the
 * target schema - collapsing the {@code T}/symbolic sets, binding labels by name to the real {@link Attribute}
 * instances, and reporting unresolved {@code BY} labels for null-filling. Their output, {@link ResolvedAttributes},
 * is the <b>target representation</b> the plan builder consumes; unlike the attributes, it does not propagate up the
 * tree.
 *
 * <h2>Exclusions: inherited vs synthesized</h2>
 * {@code WITHOUT} dimensions are tracked twice, on purpose, because two distinct consumers need two distinct facts:
 * <ul>
 *   <li>{@link InheritedAttributes#accumulatedExclusions} accumulates <em>every</em> dimension dropped on the way down. The
 *   innermost aggregate (which owns the physical {@code _timeseries} grouping) needs this full set to build its
 *   {@code TimeSeriesWithout}; the translator hands it to {@link SynthesizedAttributes#translateLeaf} via
 *   {@link InheritedAttributes#pathExclusions}.</li>
 *   <li>{@link SynthesizedAttributes#hasExclusions} reports whether <em>this subtree</em> contains a {@code WITHOUT}. An
 *   outer aggregate reads only that boolean to decide whether the child's {@code _timeseries} hides dimensions that
 *   must be packed and unpacked around grouping.</li>
 * </ul>
 *
 * <h2>Worked examples</h2>
 * Each aggregate's synthesized {@code [grouping, output, subtreeWithouts]} is shown on its closing line; the leaf's
 * {@code translateLeaf} additionally receives the demand's accumulated exclusions.
 * <pre>
 * sum without(pod) (
 *   avg without(region) (
 *     cpu_util
 *   ) [G=T\{region}, O={}, X={region}]   // translateLeaf path-exclusions = {pod,region}
 * ) [G=T\{region,pod}, O={}, X={region,pod}]
 *
 * sum by(cluster,region) (
 *   avg without(region) (
 *     cpu_util
 *   ) [G=T\{region}, O={}, X={region}]   // translateLeaf path-exclusions = {region}
 * ) [G={cluster}, O={cluster,region}, X={}]   // region is null-filled
 *
 * sum without(pod) (
 *   avg by(cluster,pod) (
 *     cpu_util
 *   ) [G={cluster,pod}, O={cluster,pod}, X={}]   // translateLeaf path-exclusions = {pod}
 * ) [G={cluster}, O={}, X={pod}]
 * </pre>
 *
 * <p>The descent starts from {@link InheritedAttributes#unconstrained()} above the outermost aggregate: every label is in scope
 * ({@code T}) and nothing is excluded. Both attribute types are immutable; every transition returns a new value.
 */
public final class PromqlAttributesTranslationContext {

    private PromqlAttributesTranslationContext() {}

    /**
     * The full label universe {@code T} ("every runtime label"): the one set we never enumerate. It is realised
     * physically by the opaque {@code _timeseries} grouping key, so it contributes no concrete columns. Represented as
     * {@code null} to keep the carrier a plain {@code List<Attribute>}; the set helpers below give it its meaning.
     */
    private static final List<Attribute> ALL = null;

    /**
     * The <b>inherited</b> attribute: the scope a subtree must preserve, threaded down the aggregate chain. An
     * {@code InheritedAttributes} is produced by {@link #unconstrained()} above the outermost aggregate and narrowed by each
     * aggregate before being handed to its child. The leaf selector ends the descent with {@link #reflect()}.
     */
    public static final class InheritedAttributes {

        /** {@code G}: the labels still in scope and required from below ({@code ALL} until a {@code BY} narrows it). */
        private final List<Attribute> required;
        /** {@code X}: every dimension dropped by a {@code WITHOUT} on the way down, for the innermost {@code _timeseries}. */
        private final List<Attribute> accumulatedExclusions;

        private InheritedAttributes(List<Attribute> required, List<Attribute> accumulatedExclusions) {
            this.required = required;
            this.accumulatedExclusions = accumulatedExclusions;
        }

        /** Everything in scope ({@code G=T}), nothing excluded. */
        public static InheritedAttributes unconstrained() {
            return new InheritedAttributes(ALL, List.of());
        }

        /** {@code BY(W)}: only {@code W} stays in scope; accumulated exclusions are carried through unchanged. */
        public InheritedAttributes including(List<Attribute> attributes) {
            return new InheritedAttributes(canonicalizeByFieldName(attributes), accumulatedExclusions);
        }

        /** {@code WITHOUT(E)}: {@code E} drops out of scope ({@code G \ E}) and joins the accumulated exclusions. */
        public InheritedAttributes excluding(List<Attribute> attributes) {
            List<Attribute> dropped = canonicalizeByFieldName(attributes);
            return new InheritedAttributes(minus(required, dropped), union(accumulatedExclusions, dropped));
        }

        /**
         * End the descent at the leaf selector: it exposes exactly the labels demanded of it, with no exclusions of its
         * own (the selector sits below every {@code WITHOUT}).
         */
        public SynthesizedAttributes reflect() {
            return new SynthesizedAttributes(required, List.of(), List.of());
        }

        /** Every dimension excluded along the path to here, for the innermost aggregate's {@code TimeSeriesWithout}. */
        public List<Attribute> pathExclusions() {
            return accumulatedExclusions;
        }
    }

    /**
     * The <b>synthesized</b> attribute: the labels a subtree exposes, folded up the aggregate chain. A
     * {@code SynthesizedAttributes} is seeded at the leaf ({@link InheritedAttributes#reflect}, {@link #of}, {@link #none}) and
     * folded by each aggregate ({@link #foldIncluding}/{@link #foldExcluding}) until it is translated into a physical
     * {@link ResolvedAttributes}.
     */
    public static final class SynthesizedAttributes {

        /** {@code G}: the labels this subtree exposes as grouping keys ({@code ALL} for a literal/scalar). */
        private final List<Attribute> grouping;
        /** {@code O}: the labels a {@code BY} promises to expose. Empty unless this shape came from a {@code BY}. */
        private final List<Attribute> output;
        /**
         * {@code X}: dimensions dropped by a {@code WITHOUT} <em>within this subtree only</em> - not the whole-path
         * exclusions (those live on the inherited path, see {@link InheritedAttributes#accumulatedExclusions}). A {@code BY}
         * clears it. Outer aggregates read it only as a boolean via {@link #hasExclusions}.
         */
        private final List<Attribute> subtreeWithouts;

        private SynthesizedAttributes(List<Attribute> grouping, List<Attribute> output, List<Attribute> subtreeWithouts) {
            this.grouping = grouping;
            this.output = output;
            this.subtreeWithouts = subtreeWithouts;
        }

        /**
         * The shape of a subtree that exposes no specific grouping (a literal selector, a scalar). It carries the full
         * set {@code T} so that a {@code BY} stacked directly on top keeps its declared keys.
         */
        public static SynthesizedAttributes none() {
            return new SynthesizedAttributes(ALL, List.of(), List.of());
        }

        /** A shape built directly from a known label set, e.g., a bare selector's output. */
        public static SynthesizedAttributes of(List<Attribute> labels) {
            return new SynthesizedAttributes(canonicalizeByFieldName(labels), List.of(), List.of());
        }

        /**
         * Fold {@code BY(W)} over the child shape: keep the {@code BY} keys the child actually exposes
         * ({@code child.G & W}), but remember the full {@code W} so {@link #translate} can null-fill any that are missing. A
         * {@code BY} clears exclusions because it groups by concrete labels.
         */
        public static SynthesizedAttributes foldIncluding(List<Attribute> promised, SynthesizedAttributes child) {
            List<Attribute> keys = canonicalizeByFieldName(promised);
            return new SynthesizedAttributes(intersect(child.grouping, keys), keys, List.of());
        }

        /**
         * Fold {@code WITHOUT(E)} over the child shape: drop the excluded labels ({@code child.G \ E}) and accumulate
         * the exclusions ({@code child.X | E}).
         */
        public static SynthesizedAttributes foldExcluding(List<Attribute> removed, SynthesizedAttributes child) {
            List<Attribute> dropped = canonicalizeByFieldName(removed);
            return new SynthesizedAttributes(minus(child.grouping, dropped), List.of(), union(child.subtreeWithouts, dropped));
        }

        /**
         * Translate the innermost aggregate, whose child is the raw selector and which therefore owns the single
         * physical {@code _timeseries} grouping. Labels resolve against this shape's own {@link #grouping}; the
         * dimensions to exclude are the full path exclusions supplied by the matching {@link InheritedAttributes#pathExclusions}.
         */
        public ResolvedAttributes translateLeaf(List<Attribute> pathExclusions) {
            return translateAgainst(grouping, canonicalizeByFieldName(pathExclusions));
        }

        /**
         * Translate an aggregate stacked on top of another aggregate, binding it to the child plan's resolved
         * {@code childColumns} (runtime data, available only once the child has been translated). Labels resolve
         * against those columns; an empty list means "trust this shape as-is".
         */
        public ResolvedAttributes translate(List<Attribute> childColumns) {
            List<Attribute> available = childColumns.isEmpty() ? grouping : canonicalizeByFieldName(childColumns);
            return translateAgainst(available, subtreeWithouts);
        }

        /** The concrete labels this shape exposes - used to resolve an aggregate stacked directly on top of it. */
        public List<Attribute> declared() {
            return asList(grouping);
        }

        /** Whether this shape exposes any concrete label that an outer binary operator could match on. */
        public boolean hasDeclared() {
            return asList(projected()).isEmpty() == false;
        }

        /** Whether this subtree contains a {@code WITHOUT}, i.e. its {@code _timeseries} hides dimensions to pack around. */
        public boolean hasExclusions() {
            return subtreeWithouts.isEmpty() == false;
        }

        /**
         * The label set to resolve against the available output in {@link #translate}: a {@code BY} shape projects its
         * declared {@code BY} {@link #output} (so missing labels can be null-filled); every other shape projects its
         * in-scope {@link #grouping}. A non-empty {@link #output} is produced only by a {@code BY}, so it doubles as the
         * discriminator.
         */
        private List<Attribute> projected() {
            return output.isEmpty() ? grouping : output;
        }

        /**
         * Concretize {@code projected} against {@code available}: split it into concrete grouping keys, reporting any
         * {@code BY} labels that are absent as {@code absent}. When {@code _timeseries} is available it becomes the sole
         * grouping key and the concrete labels move to {@code passthrough}.
         */
        private ResolvedAttributes translateAgainst(List<Attribute> available, List<Attribute> excludedDimensions) {
            List<Attribute> projected = projected();
            Attribute ts = findByFieldName(available, MetadataAttribute.TIMESERIES);
            List<Attribute> resolved = resolveAgainst(projected, available);
            List<Attribute> missing = asList(minus(projected, resolved));

            if (ts != null) {
                List<Attribute> attrs = new ArrayList<>();
                for (Attribute attr : resolved) {
                    if (MetadataAttribute.isTimeSeriesAttributeName(attr.name()) == false) {
                        attrs.add(attr);
                    }
                }
                return new ResolvedAttributes(List.of(ts), attrs, missing, excludedDimensions);
            }

            return new ResolvedAttributes(resolved, List.of(), missing, excludedDimensions);
        }
    }

    /**
     * The <b>target representation</b> produced by {@link SynthesizedAttributes#translateLeaf}/
     * {@link SynthesizedAttributes#translate}. Translator code consumes this directly to build the aggregate plan node.
     */
    public record ResolvedAttributes(
        /*
         * Concrete grouping keys for the aggregate.
         *
         * If `_timeseries` is present, it is the grouping key and concrete label attrs
         * move to passthrough. Otherwise, these are ordinary concrete label keys.
         */
        List<Attribute> groupings,

        /*
         * Visible label attrs that belong in the output shape but are not grouping keys.
         *
         * Outer aggregation carries these through pack/unpack so they survive grouping
         * without becoming split keys.
         */
        List<Attribute> passthrough,

        /*
         * BY labels requested by the spec but absent from visible output.
         *
         * Caller turns these into null aliases, so PromQL BY preserves declared labels
         * in the final output even when they are not visible in the child.
         */
        List<Attribute> absent,

        /*
          Concrete dimensions to exclude from the `_timeseries` grouping (fed into TimeSeriesWithout).

          For the innermost aggregate (translateLeaf) this is the FULL set accumulated down the
          inherited path; for an outer aggregate (translate) it is only that subtree's own WITHOUTs,
          and a BY clears it. Whole-path exclusions therefore exist only on the inherited path and
          are consumed only by the leaf translation.
         */
        List<Attribute> excludedDimensions
    ) {}

    // ----------------------------------------------------------------
    // Label-set algebra: total operations over List<Attribute>, with the ALL sentinel for the universe T.
    // Labels are compared by field name; the operands marked "finite" are never ALL.
    // ----------------------------------------------------------------

    /**
     * Collapse a raw label list to its canonical set by field name: at most one attribute per field name, first
     * occurrence wins, insertion order preserved. Every external finite list is funnelled through here before it is
     * stored, so the algebra below can assume its inputs are already duplicate-free.
     */
    private static List<Attribute> canonicalizeByFieldName(List<Attribute> labels) {
        assert labels != ALL : "canonicalizeByFieldName expects a finite list, not T";
        List<Attribute> result = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (Attribute attr : labels) {
            if (seen.add(fieldName(attr))) {
                result.add(attr);
            }
        }
        return result;
    }

    /** {@code a | b} by field name, order-preserving ({@code a} first). Both operands are finite. */
    private static List<Attribute> union(List<Attribute> a, List<Attribute> b) {
        assert a != ALL && b != ALL : "union expects finite operands, not T";
        List<Attribute> result = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (Attribute attr : a) {
            if (seen.add(fieldName(attr))) {
                result.add(attr);
            }
        }
        for (Attribute attr : b) {
            if (seen.add(fieldName(attr))) {
                result.add(attr);
            }
        }
        return result;
    }

    /** {@code a & b}. {@code a} may be {@code T} (then the result is exactly {@code b}); {@code b} is finite. */
    private static List<Attribute> intersect(List<Attribute> a, List<Attribute> b) {
        assert b != ALL : "intersect expects a finite right operand, not T";
        if (a == ALL) {
            return new ArrayList<>(b);
        }
        Set<String> names = fieldNames(b);
        return filter(a, attr -> names.contains(fieldName(attr)));
    }

    /** {@code a \ b}. {@code a} may be {@code T} ({@code T} minus a finite set stays {@code T}); {@code b} is finite. */
    private static List<Attribute> minus(List<Attribute> a, List<Attribute> b) {
        assert b != ALL : "minus expects a finite right operand, not T";
        if (a == ALL) {
            return ALL;
        }
        Set<String> names = fieldNames(b);
        return filter(a, attr -> names.contains(fieldName(attr)) == false);
    }

    /**
     * The concrete columns {@code set} contributes when resolved against the labels {@code available} actually carries.
     * {@code T} contributes none: it is realised by the opaque {@code _timeseries} grouping, not by enumerating the
     * universe. A finite set keeps the identical attribute when present, otherwise substitutes the member of
     * {@code available} sharing its field name, dropping entries with no match.
     */
    private static List<Attribute> resolveAgainst(List<Attribute> set, List<Attribute> available) {
        if (set == ALL) {
            return List.of();
        }
        if (available == ALL) {
            return new ArrayList<>(set);
        }
        List<Attribute> resolved = new ArrayList<>();
        for (Attribute attr : set) {
            if (available.contains(attr)) {
                resolved.add(attr);
                continue;
            }
            Attribute byName = findByFieldName(available, fieldName(attr));
            if (byName != null) {
                resolved.add(byName);
            }
        }
        return resolved;
    }

    /** The member with this field name, or {@code null} (always {@code null} for {@code T}). */
    private static Attribute findByFieldName(List<Attribute> set, String name) {
        if (set == ALL) {
            return null;
        }
        for (Attribute attr : set) {
            if (fieldName(attr).equals(name)) {
                return attr;
            }
        }
        return null;
    }

    /** The concrete members, or the empty list for {@code T}. */
    private static List<Attribute> asList(List<Attribute> set) {
        return set == ALL ? List.of() : set;
    }

    private static Set<String> fieldNames(List<Attribute> set) {
        Set<String> names = new LinkedHashSet<>();
        for (Attribute attr : set) {
            names.add(fieldName(attr));
        }
        return names;
    }

    private static List<Attribute> filter(List<Attribute> set, Predicate<Attribute> keep) {
        List<Attribute> result = new ArrayList<>();
        for (Attribute attr : set) {
            if (keep.test(attr)) {
                result.add(attr);
            }
        }
        return result;
    }

    /**
     * Canonical name used by the label algebra: a {@link FieldAttribute} uses its field name, everything else falls
     * back to {@link Attribute#name()}.
     */
    private static String fieldName(Attribute attr) {
        if (attr instanceof FieldAttribute fieldAttr) {
            return fieldAttr.fieldName().string();
        }
        return attr.name();
    }
}
