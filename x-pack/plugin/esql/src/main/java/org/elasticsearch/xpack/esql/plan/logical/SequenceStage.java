/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.SequenceSourceTypes;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Resolves correlation keys in one stage's scope. The stage preserves source-event identity through projections and
 * expansion: its execution must carry the original timestamp and PIT ordinal separately from user-visible columns.
 * It does not introduce an output limit or sort into the candidate stream.
 */
public final class SequenceStage extends UnaryPlan implements PostAnalysisPlanVerificationAware, PostOptimizationVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "SequenceStage",
        SequenceStage::new
    );

    private final List<Attribute> keys;
    private final long maxGapNanos;
    private static final Set<DataType> KEY_TYPES = Set.of(
        DataType.NULL,
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.UNSIGNED_LONG,
        DataType.DOUBLE,
        DataType.KEYWORD,
        DataType.IP,
        DataType.VERSION,
        DataType.DATETIME,
        DataType.DATE_NANOS
    );

    public SequenceStage(Source source, LogicalPlan child, List<Attribute> keys) {
        this(source, child, keys, 0);
    }

    /** Limits this member's incoming link from the selected previous event; zero leaves that link unconstrained. */
    public SequenceStage(Source source, LogicalPlan child, List<Attribute> keys, long maxGapNanos) {
        super(source, child);
        if (maxGapNanos < 0) {
            throw new IllegalArgumentException("SEQUENCE link time limit must be positive or absent");
        }
        this.keys = List.copyOf(keys);
        this.maxGapNanos = maxGapNanos;
    }

    private SequenceStage(StreamInput in) throws IOException {
        this(
            readSource(in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readVLong()
        );
    }

    private static Source readSource(StreamInput in) throws IOException {
        Sequence.checkPlanSupported(in.getTransportVersion());
        return Source.readFrom((PlanStreamInput) in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Sequence.checkPlanSupported(out.getTransportVersion());
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(keys);
        out.writeVLong(maxGapNanos);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<SequenceStage> info() {
        return NodeInfo.create(this, SequenceStage::new, child(), keys, maxGapNanos);
    }

    @Override
    public SequenceStage replaceChild(LogicalPlan newChild) {
        return new SequenceStage(source(), newChild, keys, maxGapNanos);
    }

    public List<Attribute> keys() {
        return keys;
    }

    public long maxGapNanos() {
        return maxGapNanos;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(keys);
    }

    @Override
    public boolean skipTelemetry() {
        // This wrapper scopes BY resolution; SEQUENCE and the actual stage commands carry the user-facing metrics.
        return true;
    }

    /** Direct callers retain the default-off stage checks without request pragmas. */
    public void postAnalysisVerification(Failures failures) {
        postAnalysisVerification(failures, false);
    }

    /** Direct plan checkers retain the default-off behavior. */
    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return stageChecker(false);
    }

    /** Apply a request opt-in only to this stage, preserving its complete source and key validation. */
    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification(
        AnalysisRegistry analysisRegistry,
        QueryPragmas pragmas,
        EsqlFlags flags
    ) {
        return stageChecker(pragmas.sequenceUriParts());
    }

    private BiConsumer<LogicalPlan, Failures> stageChecker(boolean uriParts) {
        return (lp, failures) -> {
            if (lp == this) {
                postAnalysisVerification(failures, uriParts);
            }
        };
    }

    private void postAnalysisVerification(Failures failures, boolean uriParts) {
        for (Attribute key : keys) {
            if (supportsKeyType(key.dataType()) == false) {
                failures.add(fail(key, "SEQUENCE BY column [{}] has unsupported type [{}]", key.name(), key.dataType()));
            }
        }
        verifyEventPlan(child(), failures, uriParts);
    }

    /** Alias folding can expose an indexed search only after the analyzed stage was accepted. */
    @Override
    public void postOptimizationVerification(Failures failures) {
        verifySearchTree(child(), failures);
    }

    /** Optimization may expose indexed aliases; membership-subquery sources keep their ordinary independent rules. */
    private static void verifySearchTree(LogicalPlan plan, Failures failures) {
        if (plan instanceof AbstractSubqueryJoin join) {
            verifySearchTree(join.left(), failures);
            return;
        }
        verifySourceSearchPlacement(plan, failures);
        plan.children().forEach(child -> verifySearchTree(child, failures));
    }

    /** Scalar correlation types; multivalued positions require an explicit stage MV_EXPAND at runtime. */
    public static boolean supportsKeyType(DataType type) {
        return KEY_TYPES.contains(type);
    }

    private static void verifyEventPlan(LogicalPlan plan, Failures failures) {
        verifyEventPlan(plan, failures, false);
    }

    private static void verifyEventPlan(LogicalPlan plan, Failures failures, boolean uriParts) {
        if (plan.source().viewName() != null || plan instanceof BranchPlan view && view.viewBranchKeys().isEmpty() == false) {
            // Request filters on views apply to their computed output. The native reader currently scopes its
            // request filter to raw source events, so admitting a view could silently discard valid events.
            failures.add(fail(plan, "Views are not supported inside a SEQUENCE stage"));
            return;
        }
        // Membership subqueries are predicates on the left event stream. Their right side may legitimately aggregate.
        if (plan instanceof AbstractSubqueryJoin join) {
            verifyEventPlan(join.left(), failures, uriParts);
            return;
        }
        verifySourceSearchPlacement(plan, failures);
        if (plan instanceof EsRelation relation) {
            if (relation.originalIndices().keySet().stream().anyMatch(alias -> alias.isEmpty() == false)) {
                failures.add(fail(relation, "Cross-cluster sources are not supported inside a SEQUENCE stage"));
            }
            Attribute timestamp = relation.output().stream().filter(a -> a.name().equals("@timestamp")).findFirst().orElse(null);
            if (timestamp == null || (timestamp.dataType() != DataType.DATETIME && timestamp.dataType() != DataType.DATE_NANOS)) {
                failures.add(fail(relation, "SEQUENCE stages require a source [@timestamp] of type [date] or [date_nanos]"));
            } else if (timestamp instanceof FieldAttribute field && SequenceSourceTypes.supports(field.field()) == false) {
                // This source identity is mandatory even when a stage projection or payload pruning hides the timestamp.
                failures.add(
                    fail(relation, "SEQUENCE source [@timestamp] requires a consistent mapped type without per-index conversions")
                );
            }
            return;
        }
        if (plan instanceof Eval eval) {
            eval.fields().stream().filter(a -> a.name().equals("@timestamp")).forEach(a -> {
                failures.add(fail(a, "SEQUENCE stages cannot overwrite the source [@timestamp]"));
            });
        }
        if (plan instanceof RegexExtract extract) {
            extract.extractedFields().stream().filter(a -> a.name().equals("@timestamp")).forEach(a -> {
                failures.add(fail(a, "SEQUENCE stages cannot overwrite the source [@timestamp]"));
            });
        }
        if (plan instanceof Project project) {
            for (var expression : project.projections()) {
                if (expression instanceof Alias alias
                    && alias.name().equals("@timestamp")
                    && isSourceTimestamp(alias.child(), project.child()) == false) {
                    failures.add(fail(alias, "SEQUENCE stages cannot overwrite the source [@timestamp]"));
                }
            }
        }
        if (plan instanceof Filter
            || plan instanceof SequenceCondition
            || plan instanceof Eval
            || plan instanceof Project
            || plan instanceof MvExpand
            || plan instanceof Dissect
            || plan instanceof Grok
            || (uriParts && plan instanceof UriParts)
            || plan instanceof Subquery
            || plan instanceof UnionAll) {
            plan.children().forEach(child -> verifyEventPlan(child, failures, uriParts));
        } else {
            failures.add(fail(plan, "[{}] is not supported inside a SEQUENCE stage", plan.nodeName()));
        }
    }

    private static void verifySourceSearchPlacement(LogicalPlan plan, Failures failures) {
        List<Expression> sourceSearches = plan instanceof Filter filter
            ? Predicates.splitAnd(filter.condition())
                .stream()
                .filter(predicate -> SequenceStageProjection.isExactSourceSearchPredicate(filter, predicate))
                .toList()
            : List.of();
        if (sourceSearches.isEmpty() == false
            && plan instanceof Filter filter
            && filter.child().output().stream().anyMatch(MetadataAttribute::isScoreAttribute)) {
            failures.add(
                fail(plan, "Indexed full-text search in a SEQUENCE stage does not support METADATA [_score]; remove that metadata request")
            );
        }
        plan.forEachExpression(FullTextFunction.class, function -> {
            if (function.isRuntimeSearch() == false && sourceSearches.stream().noneMatch(predicate -> predicate == function)) {
                failures.add(
                    fail(
                        function,
                        "Indexed search function [{}] in a SEQUENCE stage requires an exact top-level AND term in a WHERE directly after FROM; "
                            + "OR or NOT outside the search string is not supported",
                        function.functionName()
                    )
                );
            }
        });
    }

    private static boolean isSourceTimestamp(Expression expression, LogicalPlan plan) {
        if (expression instanceof Alias alias) {
            return isSourceTimestamp(alias.child(), plan);
        }
        if (expression instanceof Attribute == false) {
            return false;
        }
        if (plan instanceof EsRelation relation) {
            return relation.output().stream().anyMatch(a -> a.name().equals("@timestamp") && a.semanticEquals(expression));
        }
        if (plan instanceof AbstractSubqueryJoin join) {
            return isSourceTimestamp(expression, join.left());
        }
        if (plan instanceof UnionAll union) {
            // Union outputs have their own IDs. Alignment preserves column positions across the branches.
            for (int column = 0; column < union.output().size(); column++) {
                if (union.output().get(column).semanticEquals(expression)) {
                    final int position = column;
                    return union.children().isEmpty() == false && union.children().stream().allMatch(child -> {
                        return child.output().size() > position && isSourceTimestamp(child.output().get(position), child);
                    });
                }
            }
            return false;
        }
        if (plan instanceof Project project) {
            for (var projection : project.projections()) {
                if (projection instanceof Alias alias && alias.toAttribute().semanticEquals(expression)) {
                    return isSourceTimestamp(alias.child(), project.child());
                }
            }
        }
        if (plan instanceof Eval eval) {
            for (Alias alias : eval.fields()) {
                if (alias.toAttribute().semanticEquals(expression)) {
                    return isSourceTimestamp(alias.child(), eval);
                }
            }
        }
        return plan.children().isEmpty() == false && plan.children().stream().allMatch(child -> isSourceTimestamp(expression, child));
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), keys, maxGapNanos);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj
            || obj instanceof SequenceStage other
                && child().equals(other.child())
                && keys.equals(other.keys)
                && maxGapNanos == other.maxGapNanos;
    }
}
