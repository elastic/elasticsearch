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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedSingleTypeEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.List;

/**
 * Hands HIGHLIGHT each row's {@code _index} when the queried indices disagree on an ON field's analyzer, so the row is
 * highlighted with the analyzer of the index it came from instead of {@code standard}.
 * <p>
 * The key is a synthetic alias of {@code _index} evaluated right above the relation and carried through every
 * projection up to HIGHLIGHT; being synthetic, {@code UnionTypesCleanup} keeps it out of the final output, and a user
 * {@code METADATA _index} that was renamed or dropped stays renamed or dropped. Rows that STATS, ROW, FORK and the like
 * produce have no single source index, so those plans keep the {@code standard} fallback and its warning.
 */
public class ResolveHighlightIndexKey extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    public static final String INDEX_KEY_NAME = "$$_index$highlight";

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
        if (highlight.options() != null && highlight.options().get(Highlight.ANALYZER) != null) {
            return false; // WITH analyzer applies to every row
        }
        return highlight.fields().stream().anyMatch(ResolveHighlightIndexKey::hasPerIndexAnalyzers);
    }

    private static boolean hasPerIndexAnalyzers(NamedExpression field) {
        EsField esField = field instanceof FieldAttribute fa ? fa.field() : null;
        // UnionTypesCleanup has not unwrapped partially unmapped fields yet.
        if (esField instanceof PotentiallyUnmappedSingleTypeEsField punk) {
            esField = punk.mappedField();
        }
        return esField instanceof TextEsField text && text.analyzerGroups() != null;
    }

    /**
     * Returns {@code plan} with the key in its output, setting {@code key} on the way up from the relation, or
     * {@code null} when its rows have no single source index.
     */
    private static LogicalPlan withIndexKey(LogicalPlan plan, Holder<Attribute> key) {
        LogicalPlan result = switch (plan) {
            case EsRelation relation -> {
                Attribute index = relation.output()
                    .stream()
                    .filter(a -> a instanceof MetadataAttribute && a.name().equals(MetadataAttribute.INDEX))
                    .findFirst()
                    .orElse(null);
                if (index == null) {
                    index = new MetadataAttribute(
                        relation.source(),
                        MetadataAttribute.INDEX,
                        DataType.KEYWORD,
                        Nullability.TRUE,
                        null,
                        true,
                        true
                    );
                    relation = relation.withAdditionalAttribute(index);
                }
                Alias alias = new Alias(relation.source(), INDEX_KEY_NAME, index, null, true);
                key.set(alias.toAttribute());
                yield new Eval(relation.source(), relation, List.of(alias));
            }
            case Project project -> {
                LogicalPlan child = withIndexKey(project.child(), key);
                if (child == null) {
                    yield null;
                }
                List<NamedExpression> projections = new ArrayList<>(project.projections());
                projections.add(key.get());
                yield project.replaceChild(child).withProjections(projections);
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
            default -> null; // ROW, FORK, subqueries: no single source index per row
        };
        // Aggregations drop the key from the output.
        return result != null && result.outputSet().contains(key.get()) ? result : null;
    }
}
