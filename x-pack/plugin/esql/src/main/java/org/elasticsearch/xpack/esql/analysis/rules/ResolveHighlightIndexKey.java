/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Dedup;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.highlight.HighlightAnalyzers;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * When the queried indices disagree on an ON field's analyzer, passes HIGHLIGHT each row's {@code _index}, so HIGHLIGHT
 * uses the analyzer of the row's index instead of {@code standard}.
 * <p>
 * The key is a synthetic alias of {@code _index}. This rule evaluates it right above each relation and adds it to every
 * projection up to HIGHLIGHT. A projection above HIGHLIGHT restores HIGHLIGHT's original output, so later commands never
 * see the added columns. A user {@code METADATA _index} that was renamed or dropped stays renamed or dropped.
 * <p>
 * STATS and ROW rows have no single source index, and DEDUP would group by the key. Those plans keep the
 * {@code standard} fallback and its warning.
 */
public class ResolveHighlightIndexKey extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    public static final String INDEX_KEY_NAME = Attribute.rawTemporaryName(MetadataAttribute.INDEX, "highlight");

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        if (context.minimumVersion().supports(TextEsField.TEXT_FIELD_ANALYZER) == false) {
            return plan; // an older node could not read the key off the plan
        }
        return plan.transformUp(Highlight.class, highlight -> {
            if (highlight.indexKey() != null || highlight.resolved() == false || highlight.hasAnalyzerOption()) {
                return highlight; // WITH analyzer applies to every row
            }
            List<NamedExpression> grouped = highlight.fields()
                .stream()
                .filter(field -> HighlightAnalyzers.analyzerGroups(field, highlight.fieldMappings()) != null)
                .toList();
            // The key only holds the indices the rows are read from, which a LOOKUP JOIN field's groups do not name.
            // ponytail: one such field keeps every ON field on the fallback. Routing per field needs HIGHLIGHT to know
            // which fields the key covers.
            if (grouped.isEmpty() == false && grouped.stream().allMatch(f -> rowSourceOf(highlight.child(), f.toAttribute()) != null)) {
                LogicalPlan child = withIndexKey(highlight.child());
                if (child != null) {
                    // Project away the key and any added _index, which a later DEDUP would group by.
                    return new Project(highlight.source(), highlight.withIndexKey(child, indexKey(child)), highlight.output());
                }
            }
            return highlight;
        });
    }

    /**
     * The relation, or nested FORK or UNION ALL, that produces the rows of {@code plan}. {@link #withIndexKey} reads their
     * {@code _index} there.
     */
    private static @Nullable LogicalPlan rowSource(LogicalPlan plan) {
        return switch (plan) {
            case EsRelation relation -> relation;
            case LeafPlan ignored -> null;
            case UnaryPlan unary -> rowSource(unary.child());
            case BinaryPlan binary -> rowSource(binary.left());
            case MergePlan merge -> merge;
            default -> throw new IllegalStateException("unexpected plan [" + plan.nodeName() + "] under HIGHLIGHT");
        };
    }

    /** The {@link #rowSource} of {@code plan} when {@code column} is read off it, so the rows' {@code _index} names its index. */
    static @Nullable LogicalPlan rowSourceOf(LogicalPlan plan, Attribute column) {
        LogicalPlan source = rowSource(plan);
        return source != null && source.outputSet().contains(column) ? source : null;
    }

    /** Returns {@code plan} with the key in its output, or {@code null} when its rows have no single source index. */
    private static LogicalPlan withIndexKey(LogicalPlan plan) {
        // Reuse the key of an earlier HIGHLIGHT. A second alias of the same name would shadow it in Eval's output.
        if (indexKey(plan) != null) {
            return plan;
        }
        LogicalPlan result = switch (plan) {
            case EsRelation relation -> {
                Attribute index = firstNamed(relation.output(), MetadataAttribute.INDEX, a -> a instanceof MetadataAttribute);
                if (index == null) {
                    index = new MetadataAttribute(relation.source(), MetadataAttribute.INDEX, KEYWORD, Nullability.TRUE, null, true, true);
                    relation = relation.withAdditionalAttribute(index);
                }
                Alias alias = new Alias(relation.source(), INDEX_KEY_NAME, index, null, true);
                yield new Eval(relation.source(), relation, List.of(alias));
            }
            case LeafPlan ignored -> null; // ROW, LocalRelation and ExternalRelation rows have no source index
            case Project project -> {
                LogicalPlan child = withIndexKey(project.child());
                if (child == null) {
                    yield null;
                }
                List<NamedExpression> projections = CollectionUtils.combine(project.projections(), indexKey(child));
                // A user KEEP stays a Keep, because UnionTypesCleanup only reads explicitly kept virtual columns from Keep nodes.
                yield project instanceof Keep
                    ? new Keep(project.source(), child, projections)
                    : new Project(project.source(), child, projections);
            }
            // DEDUP groups by every column of its input, so the key would split rows that only differ by index.
            case Dedup ignored -> null;
            case InlineStats inlineStats -> {
                // INLINE STATS keeps every input row, so the key goes into the aggregate's input rather than its output.
                Aggregate aggregate = inlineStats.aggregate();
                LogicalPlan child = withIndexKey(aggregate.child());
                yield child == null ? null : inlineStats.replaceChild(aggregate.replaceChild(child));
            }
            case UnaryPlan unary -> {
                LogicalPlan child = withIndexKey(unary.child());
                yield child == null ? null : unary.replaceChild(child);
            }
            case BinaryPlan binary -> {
                // Rows come from the left side; the right side is a lookup index or an inline aggregation.
                LogicalPlan left = withIndexKey(binary.left());
                yield left == null ? null : binary.replaceChildren(left, binary.right());
            }
            default -> throw new IllegalStateException("unexpected plan [" + plan.nodeName() + "] under HIGHLIGHT");
        };
        // Aggregations drop the key from the output.
        return result != null && indexKey(result) != null ? result : null;
    }

    /** The key in {@code plan}'s output. Matching by name is safe because only this rule makes a synthetic column of that name. */
    private static @Nullable Attribute indexKey(LogicalPlan plan) {
        return firstNamed(plan.output(), INDEX_KEY_NAME, Attribute::synthetic);
    }

    private static Attribute firstNamed(List<Attribute> attributes, String name, Predicate<Attribute> filter) {
        return attributes.stream().filter(a -> a.name().equals(name) && filter.test(a)).findFirst().orElse(null);
    }
}
