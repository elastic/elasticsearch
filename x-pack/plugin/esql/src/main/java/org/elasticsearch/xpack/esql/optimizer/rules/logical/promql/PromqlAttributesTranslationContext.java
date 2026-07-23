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
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.PROMETHEUS_LABELS_PREFIX;

/**
 * The label-grouping fragment of the PromQL -> ESQL syntax-directed translation.
 * <p>
 * A PromQL query such as {@code sum by(cluster) (avg without(region) (cpu_util))} is a chain of aggregates over a single
 * leaf selector. PromQL grouping is <em>dynamic</em>: the concrete set of label names lives on disk and is unknown at
 * plan time, so we cannot list it efficiently. Instead, the translation carries the aggregation shape symbolically
 * and resolves it against real columns only at the very end.
 *
 * <h2>Abstract domain: label sets</h2>
 * A label set is a plain {@code List<Attribute>} plus two "top" sentinels: {@code UNIVERSE} for the full label universe
 * {@code T} ("every runtime label") - the one set we can never enumerate at plan time - and {@code SCALAR} for a
 * subtree that exposes no series identity at all (a literal/scalar or a bare {@code NONE}). {@code T} is realised
 * physically by the opaque {@code _timeseries} grouping key (see {@code TranslateTimeSeriesWithout}), which is why it
 * contributes no concrete columns when resolved. The static {@code union}/{@code intersect}/{@code minus} helpers
 * implement set algebra over these values (labels are compared by field name). {@code T} minus a finite set stays
 * {@code T}: the removed labels are never listed, and - because every {@code BY} forces a finite scope down its
 * subtree - that complement is never observed, so a single {@code UNIVERSE} sentinel is enough.
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

    /**
     * The full label universe {@code T} ("every runtime label"): the one set we never enumerate at plan time. It is
     * realised physically by the opaque {@code _timeseries} grouping key, so it contributes no concrete columns.
     */
    private static final List<Attribute> UNIVERSE = List.of(new ReferenceAttribute(Source.EMPTY, ":U", DataType.NULL));

    /**
     * The <b>scalar</b> sentinel: a subtree that exposes no series identity at all (a literal/scalar, or a bare
     * {@code NONE} aggregate that collapses every series into one). Like {@code UNIVERSE} it is "top" for the set
     * operations - a {@code BY} stacked on top keeps its declared keys - but, unlike {@code UNIVERSE}, it must
     * <b>not</b> materialise a {@code _timeseries} grouping. Telling it apart from {@code UNIVERSE} is what lets an
     * empty {@code without ()} (which retains the full universe {@code T}) be distinguished from {@code none()}
     * (which retains nothing).
     */
    private static final List<Attribute> SCALAR = List.of(new ReferenceAttribute(Source.EMPTY, ":S", DataType.NULL));

    private PromqlAttributesTranslationContext() {}

    /** Whether {@code set} is one of the two top sentinels ({@code UNIVERSE} or {@code SCALAR}) rather than a finite, enumerable set. */
    private static boolean isTop(List<Attribute> set) {
        return set == UNIVERSE || set == SCALAR;
    }

    // label-set algebra:

    /**
     * Collapse a raw label list to its canonical set by field name: at most one attribute per field name, first
     * occurrence wins, insertion order preserved. Every external finite list is funnelled through here before it is
     * stored, so the algebra below can assume its inputs are already duplicate-free.
     */
    private static List<Attribute> canonicalizeByFieldName(List<Attribute> labels) {
        assert isTop(labels) == false : "canonicalizeByFieldName expects a finite list, not a top sentinel";
        List<Attribute> result = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (Attribute attr : labels) {
            if (seen.add(canonicalName(attr))) {
                result.add(attr);
            }
        }
        return result;
    }

    /** {@code a | b} by field name, order-preserving ({@code a} first). Both operands are finite. */
    static List<Attribute> union(List<Attribute> a, List<Attribute> b) {
        assert isTop(a) == false && isTop(b) == false : "union expects finite operands, not a top sentinel";
        List<Attribute> result = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (Attribute attr : a) {
            if (seen.add(canonicalName(attr))) {
                result.add(attr);
            }
        }
        for (Attribute attr : b) {
            if (seen.add(canonicalName(attr))) {
                result.add(attr);
            }
        }
        return result;
    }

    /** {@code a & b}. {@code a} may be a top sentinel ({@code T} or scalar; then the result is exactly {@code b}); {@code b} is finite. */
    private static List<Attribute> intersect(List<Attribute> a, List<Attribute> b) {
        assert isTop(b) == false : "intersect expects a finite right operand, not a top sentinel";
        if (isTop(a)) {
            return new ArrayList<>(b);
        }
        Set<String> names = fieldNames(b);
        return filter(a, attr -> names.contains(canonicalName(attr)));
    }

    /** {@code a \ b}. {@code a} may be a top sentinel (top minus a finite set stays that same sentinel); {@code b} is finite. */
    private static List<Attribute> minus(List<Attribute> a, List<Attribute> b) {
        assert isTop(b) == false : "minus expects a finite right operand, not a top sentinel";
        if (isTop(a)) {
            return a;
        }
        Set<String> names = fieldNames(b);
        return filter(a, attr -> names.contains(canonicalName(attr)) == false);
    }

    /** The member with this field name, or {@code null} (always {@code null} for {@code T}). */
    static Attribute findByFieldName(List<Attribute> set, String name) {
        if (isTop(set)) {
            return null;
        }
        for (Attribute attr : set) {
            if (canonicalName(attr).equals(name)) {
                return attr;
            }
        }
        return null;
    }

    /** The concrete members, or the empty list for a top sentinel ({@code T} or scalar). */
    private static List<Attribute> asList(List<Attribute> set) {
        return isTop(set) ? List.of() : set;
    }

    private static Set<String> fieldNames(List<Attribute> set) {
        Set<String> names = new LinkedHashSet<>(set.size());
        for (Attribute attr : set) {
            names.add(canonicalName(attr));
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
     * back to {@link Attribute#name()}. PromQL refers to labels by bare names ({@code le}, {@code job}), while TSDB
     * dimensions that store Prometheus labels can be exposed as {@code labels.le} / {@code labels.job}. Strip that
     * storage prefix so {@code by}, {@code without}, intersection, and difference compare PromQL label keys rather than
     * backing field names (and so exclusion names match the dimensions the {@code _timeseries} block loader enumerates).
     */
    static String canonicalName(Attribute attr) {
        return stripIgnorePrefix((attr instanceof FieldAttribute fa) ? fa.fieldName().string() : attr.name());
    }

    private static String stripIgnorePrefix(String name) {
        return name.startsWith(PROMETHEUS_LABELS_PREFIX) ? name.substring(PROMETHEUS_LABELS_PREFIX.length()) : name;
    }

    /** The concrete dimension fields among {@code attributes} (used to seed a child demand from a selector's output). */
    static List<Attribute> filterDimensionAttributes(List<Attribute> attributes) {
        return attributes.stream().filter(a -> a instanceof FieldAttribute fa && fa.isDimension()).toList();
    }

    /**
     * The <b>inherited</b> attribute: the scope a subtree must preserve, threaded down the aggregate chain. An
     * {@code InheritedAttributes} is produced by {@link #unconstrained()} above the outermost aggregate and narrowed by each
     * aggregate before being handed to its child. The leaf selector ends the descent with {@link #reflect()}.
     */
    public static final class InheritedAttributes {

        /** {@code G}: the labels still in scope and required from below ({@code UNIVERSE} until a {@code BY} narrows it). */
        private final List<Attribute> required;
        /** {@code X}: every dimension dropped by a {@code WITHOUT} on the way down, for the innermost {@code _timeseries}. */
        private final List<Attribute> accumulatedExclusions;

        private InheritedAttributes(List<Attribute> required, List<Attribute> accumulatedExclusions) {
            this.required = required;
            this.accumulatedExclusions = accumulatedExclusions;
        }

        /** Everything in scope ({@code G=T}), nothing excluded. */
        public static InheritedAttributes unconstrained() {
            return new InheritedAttributes(UNIVERSE, List.of());
        }

        /**
         * {@code BY(W)}: the child scope becomes exactly {@code W}, and the accumulated exclusions are <b>cleared</b>. A
         * {@code BY} fixes its subtree's output to concrete keys, so any outer {@code WITHOUT} applies over those concrete
         * labels (handled by the outer aggregate), not as a leaf {@code _timeseries} exclusion. Carrying the outer
         * exclusions past a {@code BY} would wrongly inject a {@code _timeseries} into the inner concrete aggregate, making
         * it group by {@code identity \ excluded} instead of {@code W} and leaking every other label.
         */
        public InheritedAttributes limitedTo(List<Attribute> labels) {
            return new InheritedAttributes(canonicalizeByFieldName(labels), List.of());
        }

        /** {@code WITHOUT(E)}: {@code E} drops out of scope ({@code G \ E}) and joins the accumulated exclusions. */
        public InheritedAttributes excluding(List<Attribute> labels) {
            List<Attribute> dropped = canonicalizeByFieldName(labels);
            return new InheritedAttributes(minus(required, dropped), union(accumulatedExclusions, dropped));
        }

        /**
         * A function-internal requirement (e.g. {@code histogram_quantile} materializing the {@code le} bucket label):
         * keep the current scope and additionally require {@code labels}. Unlike {@link #limitedTo} this <b>widens</b> the
         * scope rather than replacing it; like {@code BY} it re-fixes the subtree to concrete keys and so <b>clears</b> the
         * accumulated exclusions (the function regroups by concrete labels, so an outer {@code WITHOUT} applies over those
         * keys at the outer aggregate, not as a leaf {@code _timeseries} exclusion). When the current scope is the full
         * universe {@code T} the labels already cover it, so it collapses to exactly {@code labels}.
         */
        public InheritedAttributes including(List<Attribute> labels) {
            List<Attribute> add = canonicalizeByFieldName(labels);
            return new InheritedAttributes(isTop(required) ? add : union(required, add), List.of());
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

        /** {@code G}: the labels this subtree exposes as grouping keys ({@code SCALAR} for a literal/scalar). */
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
         * The shape of a subtree that exposes no specific grouping (a literal selector, a scalar, a bare {@code NONE}
         * aggregate). It carries the {@code SCALAR} sentinel: top-like for set operations (so a {@code BY} stacked directly
         * on top keeps its declared keys) but, unlike {@code T}, it does not materialise a {@code _timeseries} grouping.
         */
        public static SynthesizedAttributes none() {
            return new SynthesizedAttributes(SCALAR, List.of(), List.of());
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
            var timeseries = findByFieldName(available, MetadataAttribute.TIMESERIES);

            /*
             * `without` means "group by the runtime label set", represented by `_timeseries`.
             * If the input plan does not expose `_timeseries` yet, synthesize it here so the
             * plan builder can lower it normally. Do not do this for the scalar/NONE sentinel.
             */
            if (timeseries == null && projected == UNIVERSE) {
                timeseries = FieldAttribute.timeSeriesAttribute(Source.EMPTY);
            }

            var resolved = new ArrayList<Attribute>();

            if (isTop(projected) == false) {
                if (available == UNIVERSE) {
                    resolved.addAll(projected);
                } else {
                    resolved.ensureCapacity(projected.size());
                    for (Attribute attr : projected) {
                        Attribute match = available.contains(attr) ? attr : findByFieldName(available, canonicalName(attr));

                        if (match != null) {
                            resolved.add(match);
                        }
                    }
                }
            }

            var missing = asList(minus(projected, resolved));

            if (timeseries == null) {
                return new ResolvedAttributes(resolved, List.of(), missing, excludedDimensions);
            }

            resolved.removeIf(attr -> MetadataAttribute.isTimeSeriesAttributeName(attr.name()));

            return new ResolvedAttributes(List.of(timeseries), resolved, missing, excludedDimensions);
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
}
