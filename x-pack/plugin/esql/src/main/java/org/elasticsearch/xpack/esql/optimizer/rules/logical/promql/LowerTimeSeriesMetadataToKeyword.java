/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.AbstractTimeSeriesFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.ToTimeSeries;
import org.elasticsearch.xpack.esql.plan.logical.DistinctBy;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lowers the typed time-series metadata of a translated PromQL plan to the keyword {@code _timeseries}, for queries where
 * some participating node cannot read the typed form ({@link PromqlCommand#usesTimeSeriesMetadata}).
 * <p>
 * Translation and {@link ResolvePromqlLabelRefs} produce one representation: the series read their metadata as keyword
 * views of the source, cast to typed metadata above the collapse ({@link ToTimeSeries}). The keyword form is that plan
 * without the casts: each view is the keyword column it was cast from, and the metadata of a series without labels is
 * null. An {@code or} keeps one series per label set on its metadata as a whole, {@code __name__} included, rather than
 * on the match key that leaves {@code __name__} out: the verifier admits on keyword metadata only the unions where the
 * two agree. A key that compares labels in slots of their own compares them in its metadata too, rather than leaving
 * them out of it, as keyword vector matching always has. A check that a result holds one series per label set goes where
 * keyword metadata cannot state the label set, as it always has. What has no keyword form fails: reading a label out of
 * metadata, or leaving labels out of it after the collapse ({@link AbstractTimeSeriesFunction}).
 * <p>
 * The rule goes once the minimum supported version reads typed metadata.
 */
public final class LowerTimeSeriesMetadataToKeyword extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        if (PromqlCommand.usesTimeSeriesMetadata(context) || plan.anyMatch(LowerTimeSeriesMetadataToKeyword::readsTyped) == false) {
            return plan;
        }
        return lower(plan);
    }

    /** {@code plan} over keyword metadata. */
    static LogicalPlan lower(LogicalPlan plan) {
        Lowering lowering = new Lowering();
        return lowering.lower(dropUnread(lowering.keyOnMetadata(withoutUnstatedChecks(matchOnMetadata(plan)))));
    }

    /** Every {@code or} keeping its series apart by their metadata, without a match key of its own. */
    private static LogicalPlan matchOnMetadata(LogicalPlan plan) {
        if (plan.anyMatch(
            node -> node instanceof UnionAll union && union.output().stream().anyMatch(LowerTimeSeriesMetadataToKeyword::isMatchKey)
        ) == false) {
            return plan;
        }
        return plan.transformUp(node -> switch (node) {
            case TopNBy top when top.child() instanceof UnionAll union -> {
                List<Expression> groupings = top.groupings()
                    .stream()
                    .map(grouping -> grouping instanceof Attribute key && isMatchKey(key) ? metadataOf(union) : grouping)
                    .toList();
                yield new TopNBy(top.source(), union, top.order(), top.limitPerGroup(), groupings);
            }
            case UnionAll union -> union.replaceSubPlansAndOutput(
                union.children(),
                union.output().stream().filter(column -> isMatchKey(column) == false).toList()
            );
            case Project project when project.projections().stream().anyMatch(LowerTimeSeriesMetadataToKeyword::isMatchKey) -> project
                .withProjections(project.projections().stream().filter(projection -> isMatchKey(projection) == false).toList());
            default -> node;
        });
    }

    private static Attribute metadataOf(UnionAll union) {
        return union.output()
            .stream()
            .filter(column -> isTyped(column) && isMatchKey(column) == false)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("an or with a match key holds metadata [" + union + "]"));
    }

    private static boolean isMatchKey(NamedExpression projection) {
        return projection.name().equals(VectorBinarySet.OR_MATCH_KEY);
    }

    /**
     * {@code plan} without the checks for repeated label sets whose label set reads metadata derived after the collapse: a
     * keyword key cannot state it.
     */
    private static LogicalPlan withoutUnstatedChecks(LogicalPlan plan) {
        Set<NameId> derived = new HashSet<>();
        plan.forEachUp(Eval.class, eval -> {
            for (Alias field : eval.fields()) {
                if (field.child()
                    .anyMatch(e -> e instanceof AbstractTimeSeriesFunction || (e instanceof Attribute a && derived.contains(a.id())))) {
                    derived.add(field.id());
                }
            }
        });
        if (derived.isEmpty()) {
            return plan;
        }
        return plan.transformUp(
            DistinctBy.class,
            check -> check.failOnDuplicate()
                && check.child() instanceof PackDims key
                && key.packed().id().equals(check.key().id())
                && key.dims().stream().anyMatch(dim -> derived.contains(dim.id())) ? key.child() : check
        );
    }

    /** {@code plan} without the typed metadata it computes and never reads: such a column needs no keyword form. */
    private static LogicalPlan dropUnread(LogicalPlan plan) {
        while (true) {
            Map<NameId, Integer> reads = new HashMap<>();
            plan.forEachExpressionDown(Attribute.class, attribute -> reads.merge(attribute.id(), 1, Integer::sum));
            LogicalPlan pruned = plan.transformUp(Eval.class, eval -> {
                List<Alias> fields = eval.fields().stream().filter(field -> isRead(field, reads) || readsTyped(field) == false).toList();
                return fields.size() == eval.fields().size() ? eval
                    : fields.isEmpty() ? eval.child()
                    : new Eval(eval.source(), eval.child(), fields);
            });
            if (pruned == plan) {
                return plan;
            }
            plan = pruned;
        }
    }

    /** Whether something other than {@code field} itself reads it: a binding may reuse the id of the column it renames. */
    private static boolean isRead(Alias field, Map<NameId, Integer> reads) {
        int own = field.child().collect(Attribute.class).stream().mapToInt(attribute -> attribute.id().equals(field.id()) ? 1 : 0).sum();
        return reads.getOrDefault(field.id(), 0) > own;
    }

    private static boolean readsTyped(Expression expression) {
        return expression.anyMatch(LowerTimeSeriesMetadataToKeyword::isTyped);
    }

    private static boolean readsTyped(LogicalPlan node) {
        return node.expressions().stream().anyMatch(LowerTimeSeriesMetadataToKeyword::readsTyped);
    }

    private static boolean isTyped(Expression expression) {
        return expression.resolved() && expression.dataType() == DataType.TIMESERIES_DATA_TYPE;
    }

    /** A cast of keyword metadata to typed, or the rendering of typed metadata as keyword: both are the keyword column. */
    private static boolean isCast(Expression expression) {
        return (expression instanceof ToTimeSeries cast && cast.field() instanceof Literal == false)
            || (expression instanceof ToString render && isTyped(render.field()));
    }

    /** One bottom-up pass; each node reads the columns of its input as they were lowered. */
    private static final class Lowering {
        /** Each column of a lowered node, by id. */
        private final Map<NameId, Attribute> lowered = new HashMap<>();
        /** The keyword column each dropped cast stood for, by the cast's id. */
        private final Map<NameId, Attribute> casts = new HashMap<>();
        /** Metadata keys that keep every label they left out, by id: each is the metadata it was derived from. */
        private final Set<NameId> keptWhole = new HashSet<>();
        /** Projections left bare by dropping the casts over them: a projection above only reorders their columns. */
        private final Set<LogicalPlan> bare = Collections.newSetFromMap(new IdentityHashMap<>());

        /**
         * {@code plan} with each packed key's metadata keeping the labels the key compares in slots of their own: keyword
         * metadata cannot leave them out, and comparing them twice keeps the key it always had.
         */
        LogicalPlan keyOnMetadata(LogicalPlan plan) {
            Map<NameId, Set<String>> slotted = new HashMap<>();
            plan.forEachDown(PackDims.class, pack -> {
                Set<String> labels = new HashSet<>();
                for (Attribute dim : pack.dims()) {
                    if (isTyped(dim) == false) {
                        labels.add(PromqlLabels.labelName(dim));
                    }
                }
                for (Attribute dim : pack.dims()) {
                    if (isTyped(dim)) {
                        slotted.computeIfAbsent(dim.id(), id -> new HashSet<>()).addAll(labels);
                    }
                }
            });
            if (slotted.isEmpty()) {
                return plan;
            }
            return plan.transformUp(Eval.class, eval -> {
                List<Alias> fields = eval.fields().stream().map(field -> {
                    if (slotted.containsKey(field.id()) && field.child() instanceof TimeSeriesUnset unset) {
                        List<Expression> left = unset.children()
                            .subList(1, unset.children().size())
                            .stream()
                            .filter(label -> slotted.get(field.id()).contains(BytesRefs.toString(((Literal) label).value())) == false)
                            .toList();
                        if (left.isEmpty()) {
                            keptWhole.add(field.id());
                            return field.replaceChild(unset.metadata());
                        }
                        return field.replaceChild(new TimeSeriesUnset(unset.source(), unset.metadata(), left));
                    }
                    return field;
                }).toList();
                return fields.equals(eval.fields()) ? eval : new Eval(eval.source(), eval.child(), fields);
            });
        }

        LogicalPlan lower(LogicalPlan plan) {
            return plan.transformUp(node -> {
                LogicalPlan rewritten = switch (node) {
                    case Eval eval -> withoutCasts(eval);
                    case Project project -> project.withProjections(project.projections().stream().map(this::withoutCast).toList());
                    default -> node;
                };
                rewritten = rewritten.transformExpressionsOnly(Expression.class, this::lower);
                if (rewritten instanceof Project outer && bare.contains(outer.child())) {
                    rewritten = reorder(outer, (Project) outer.child());
                }
                rewritten.output().forEach(attribute -> lowered.put(attribute.id(), attribute));
                return rewritten;
            });
        }

        /** {@code eval} without the fields that only cast: their readers read the keyword column. */
        private LogicalPlan withoutCasts(Eval eval) {
            var fields = new ArrayList<Alias>();
            for (Alias field : eval.fields()) {
                if (isCast(field.child()) || keptWhole.contains(field.id())) {
                    casts.put(field.id(), (Attribute) lower(field.child()));
                } else {
                    fields.add(field);
                }
            }
            if (fields.isEmpty() && eval.child() instanceof Project projection) {
                bare.add(projection);
            }
            return fields.size() == eval.fields().size() ? eval
                : fields.isEmpty() ? eval.child()
                : new Eval(eval.source(), eval.child(), fields);
        }

        /** {@code inner} in the order {@code outer} projects its columns. */
        private static Project reorder(Project outer, Project inner) {
            Map<NameId, NamedExpression> byId = new HashMap<>();
            inner.projections().forEach(projection -> byId.put(projection.id(), projection));
            var projections = new ArrayList<NamedExpression>(outer.projections().size());
            for (NamedExpression projection : outer.projections()) {
                NamedExpression column = byId.get(projection.id());
                if (column == null || projection instanceof Attribute == false) {
                    return outer;
                }
                projections.add(column);
            }
            return new Project(outer.source(), inner.child(), projections);
        }

        private NamedExpression withoutCast(NamedExpression projection) {
            if (projection instanceof Alias alias && isCast(alias.child())) {
                Attribute column = (Attribute) lower(alias.child());
                casts.put(alias.id(), column);
                return column;
            }
            return projection;
        }

        /** {@code expression} over keyword metadata. Top-down: a cast is gone before its operand is looked at. */
        private Expression lower(Expression expression) {
            return switch (expression) {
                case ToTimeSeries cast when cast.field() instanceof Literal -> new Literal(cast.source(), null, DataType.KEYWORD);
                case ToTimeSeries cast -> lower(cast.field());
                case ToString render when isTyped(render.field()) -> lower(render.field());
                case AbstractTimeSeriesFunction function -> throw new VerificationException(
                    List.of(
                        Failure.fail(
                            function,
                            "leaving labels out of _timeseries requires typed time-series metadata [{}]",
                            function.sourceText()
                        )
                    )
                );
                case Attribute attribute when casts.containsKey(attribute.id()) -> casts.get(attribute.id());
                case Attribute attribute when isTyped(attribute) -> {
                    Attribute below = lowered.get(attribute.id());
                    yield below != null && isTyped(below) == false ? below : attribute.withDataType(DataType.KEYWORD);
                }
                default -> {
                    List<Expression> children = expression.children();
                    List<Expression> rewritten = children.stream().map(this::lower).toList();
                    yield rewritten.equals(children) ? expression : expression.replaceChildren(rewritten);
                }
            };
        }
    }
}
