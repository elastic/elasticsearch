/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
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
 * Hands HIGHLIGHT each row's {@code _index} when the queried indices disagree on an ON field's analyzer, so the row is
 * highlighted with the analyzer of the index it came from instead of {@code standard}.
 * <p>
 * The key is a synthetic alias of {@code _index} evaluated right above the relation and carried through every
 * projection up to HIGHLIGHT; being synthetic, {@code UnionTypesCleanup} keeps it out of the final output, and a user
 * {@code METADATA _index} that was renamed or dropped stays renamed or dropped. Rows that STATS, ROW, FORK and UNION ALL
 * produce have no single source index, and DEDUP would group by the key, so those plans keep the {@code standard}
 * fallback and its warning.
 */
public class ResolveHighlightIndexKey extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    public static final String INDEX_KEY_NAME = Attribute.rawTemporaryName(MetadataAttribute.INDEX, "highlight");

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        if (context.minimumVersion().supports(Highlight.ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS) == false) {
            return plan; // an older node could not read the key off the plan
        }
        return plan.transformUp(Highlight.class, highlight -> {
            if (highlight.indexKey() != null || highlight.resolved() == false || needsIndexKey(highlight) == false) {
                return highlight;
            }
            Holder<Attribute> key = new Holder<>();
            LogicalPlan child = withIndexKey(highlight.child(), key);
            return child == null ? highlight : highlight.withIndexKey(child, key.get());
        });
    }

    private static boolean needsIndexKey(Highlight highlight) {
        if (highlight.hasAnalyzerOption()) {
            return false; // WITH analyzer applies to every row
        }
        return highlight.fields().stream().anyMatch(field -> HighlightAnalyzers.analyzerGroups(field) != null);
    }

    /**
     * Returns {@code plan} with the key in its output, setting {@code key} on the way up from the relation, or
     * {@code null} when its rows have no single source index.
     */
    private static LogicalPlan withIndexKey(LogicalPlan plan, Holder<Attribute> key) {
        // Reuse the key an earlier HIGHLIGHT carries up: a second alias of the same name would shadow it in Eval's output.
        Attribute existing = firstNamed(plan.output(), INDEX_KEY_NAME, Attribute::synthetic);
        if (existing != null) {
            key.set(existing);
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
                key.set(alias.toAttribute());
                yield new Eval(relation.source(), relation, List.of(alias));
            }
            case LeafPlan ignored -> null; // ROW, LocalRelation, ExternalRelation: no source index per row
            case Project project -> {
                LogicalPlan child = withIndexKey(project.child(), key);
                if (child == null) {
                    yield null;
                }
                List<NamedExpression> projections = CollectionUtils.combine(project.projections(), key.get());
                // A user KEEP stays a Keep: UnionTypesCleanup reads the virtual columns it lists off Keep nodes only.
                yield project instanceof Keep
                    ? new Keep(project.source(), child, projections)
                    : new Project(project.source(), child, projections);
            }
            // DEDUP groups by every column of its input, so the key would split rows that only differ by index.
            case Dedup ignored -> null;
            case InlineStats inlineStats -> {
                // Every input row survives, so the key goes into the aggregate's input rather than its output.
                Aggregate aggregate = inlineStats.aggregate();
                LogicalPlan child = withIndexKey(aggregate.child(), key);
                yield child == null ? null : inlineStats.replaceChild(aggregate.replaceChild(child));
            }
            case UnaryPlan unary -> {
                LogicalPlan child = withIndexKey(unary.child(), key);
                yield child == null ? null : unary.replaceChild(child);
            }
            case BinaryPlan binary -> {
                // Rows come from the left side; the right side is a lookup index or an inline aggregation.
                LogicalPlan left = withIndexKey(binary.left(), key);
                yield left == null ? null : binary.replaceChildren(left, binary.right());
            }
            case MergePlan ignored -> null; // FORK, UNION ALL: rows come from several branches
            default -> throw new IllegalStateException("unexpected plan [" + plan.nodeName() + "] under HIGHLIGHT");
        };
        // Aggregations drop the key from the output.
        return result != null && result.outputSet().contains(key.get()) ? result : null;
    }

    private static Attribute firstNamed(List<Attribute> attributes, String name, Predicate<Attribute> filter) {
        return attributes.stream().filter(a -> a.name().equals(name) && filter.test(a)).findFirst().orElse(null);
    }
}
