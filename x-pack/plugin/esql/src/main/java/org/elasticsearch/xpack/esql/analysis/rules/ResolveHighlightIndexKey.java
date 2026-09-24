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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.IndexAnalyzerGroup;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField.UnknownAnalyzer;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Dedup;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Hands HIGHLIGHT what it needs to analyze each row with its mapping analyzer where the plan above the relation loses it.
 * <p>
 * A column FORK or UNION ALL merges is a {@link ReferenceAttribute}, which does not carry the mapping of the fields it
 * was merged from, so HIGHLIGHT gets the mapping of each such ON column: the one every branch agrees on, or a
 * {@link UnknownAnalyzer#BRANCH_CONFLICT} that falls back to {@code standard} with a warning.
 * <p>
 * When the queried indices disagree on an ON field's analyzer, HIGHLIGHT also gets each row's {@code _index}, so the row
 * is highlighted with the analyzer of the index it came from instead of {@code standard}. The key is a synthetic alias
 * of {@code _index} evaluated right above each relation and carried through every projection and every FORK or UNION ALL
 * branch up to HIGHLIGHT; being synthetic, {@code UnionTypesCleanup} keeps it out of the final output, and a user
 * {@code METADATA _index} that was renamed or dropped stays renamed or dropped. Rows that STATS and ROW produce have no
 * single source index, and DEDUP would group by the key, so those plans keep the {@code standard} fallback and its
 * warning.
 */
public class ResolveHighlightIndexKey extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    public static final String INDEX_KEY_NAME = Attribute.rawTemporaryName(MetadataAttribute.INDEX, "highlight");

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        if (context.minimumVersion().supports(Highlight.ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS) == false) {
            return plan; // an older node could not read the key or the mappings off the plan
        }
        return plan.transformUp(Highlight.class, highlight -> {
            if (highlight.indexKey() != null || highlight.resolved() == false || highlight.hasAnalyzerOption()) {
                return highlight; // WITH analyzer applies to every row
            }
            Map<String, TextEsField> mappings = mergedMappings(highlight);
            Holder<Attribute> key = new Holder<>();
            LogicalPlan child = needsIndexKey(highlight, mappings) ? withIndexKey(highlight.child(), key) : null;
            if (child != null) {
                return highlight.withIndexKeyAndMappings(child, key.get(), mappings);
            }
            return mappings.equals(highlight.fieldMappings())
                ? highlight
                : highlight.withIndexKeyAndMappings(highlight.child(), null, mappings);
        });
    }

    private static boolean needsIndexKey(Highlight highlight, Map<String, TextEsField> mappings) {
        return highlight.fields().stream().anyMatch(field -> HighlightAnalyzers.analyzerGroups(field, mappings) != null);
    }

    /** The mapping of each text ON column that comes unchanged out of a FORK or UNION ALL, by name. */
    private static Map<String, TextEsField> mergedMappings(Highlight highlight) {
        Map<String, TextEsField> mappings = new HashMap<>();
        for (NamedExpression field : highlight.fields()) {
            if (field instanceof Attribute column && column instanceof FieldAttribute == false && column.dataType() == TEXT) {
                TextEsField mapping = mergedMapping(highlight.child(), column);
                if (mapping != null) {
                    mappings.put(column.name(), mapping);
                }
            }
        }
        return Map.copyOf(mappings);
    }

    /** The mapping of {@code column}, which {@code plan} outputs, when it comes unchanged out of a FORK or UNION ALL. */
    private static @Nullable TextEsField mergedMapping(LogicalPlan plan, Attribute column) {
        if (plan instanceof MergePlan merge) {
            return branchesMapping(merge, column.name());
        }
        for (LogicalPlan child : plan.children()) {
            if (child.outputSet().contains(column)) {
                return mergedMapping(child, column);
            }
        }
        return null; // computed, e.g. by EVAL or RENAME
    }

    /**
     * The mapping the branches of {@code merge} agree on for column {@code name}, a {@link UnknownAnalyzer#BRANCH_CONFLICT}
     * when they disagree or one computes the column, or {@code null} when every branch fills it with nulls.
     */
    private static @Nullable TextEsField branchesMapping(MergePlan merge, String name) {
        TextEsField agreed = null;
        for (LogicalPlan branch : merge.children()) {
            Attribute column = firstNamed(branch.output(), name, a -> true);
            if (column == null || Fork.producesOnlyNull(branch, column)) {
                continue; // no values to analyze
            }
            TextEsField found = column instanceof FieldAttribute
                ? HighlightAnalyzers.mappingOf(column, Map.of())
                : mergedMapping(branch, column);
            TextEsField mapping = found == null
                ? null
                : mapping(name, found.analyzerName(), found.positionIncrementGap(), found.unknownAnalyzer(), found.analyzerGroups());
            if (mapping == null || (agreed != null && agreed.equals(mapping) == false)) {
                return mapping(name, null, TextEsField.DEFAULT_POSITION_INCREMENT_GAP, UnknownAnalyzer.BRANCH_CONFLICT, null);
            }
            agreed = mapping;
        }
        return agreed;
    }

    /** Only what picks the analyzer: branches that agree on it may still differ on sub-fields or doc values. */
    private static TextEsField mapping(
        String name,
        @Nullable String analyzerName,
        int positionIncrementGap,
        UnknownAnalyzer unknownAnalyzer,
        @Nullable List<IndexAnalyzerGroup> analyzerGroups
    ) {
        return new TextEsField(
            name,
            Map.of(),
            false,
            false,
            EsField.TimeSeriesFieldType.NONE,
            analyzerName,
            positionIncrementGap,
            unknownAnalyzer,
            analyzerGroups
        );
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
            case MergePlan merge -> {
                List<LogicalPlan> branches = new ArrayList<>(merge.children().size());
                for (LogicalPlan branch : merge.children()) {
                    Holder<Attribute> branchKey = new Holder<>();
                    LogicalPlan withKey = withIndexKey(branch, branchKey);
                    // Branches line up by position, so the key must come last in each, as it does in the merge output.
                    if (withKey == null || withKey.output().getLast().equals(branchKey.get()) == false) {
                        yield null;
                    }
                    branches.add(withKey);
                }
                MergePlan merged = merge.replaceSubPlans(branches).refreshOutput();
                key.set(firstNamed(merged.output(), INDEX_KEY_NAME, Attribute::synthetic));
                yield merged;
            }
            default -> throw new IllegalStateException("unexpected plan [" + plan.nodeName() + "] under HIGHLIGHT");
        };
        // Aggregations drop the key from the output.
        return result != null && result.outputSet().contains(key.get()) ? result : null;
    }

    private static Attribute firstNamed(List<Attribute> attributes, String name, Predicate<Attribute> filter) {
        return attributes.stream().filter(a -> a.name().equals(name) && filter.test(a)).findFirst().orElse(null);
    }
}
