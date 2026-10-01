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
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Hands HIGHLIGHT what it needs to analyze each row with its mapping analyzer where the plan above the relation loses it.
 * <p>
 * A column FORK or UNION ALL merges is a {@link ReferenceAttribute}, which does not carry the mapping of the fields it
 * was merged from, so HIGHLIGHT gets the mapping of each such ON column: the one every branch agrees on, one naming the
 * analyzer of each index when branches over different indices disagree, or a {@link UnknownAnalyzer#BRANCH_CONFLICT} that
 * falls back to {@code standard} with a warning. A column no branch maps is analyzed like any computed column.
 * <p>
 * When the queried indices disagree on an ON field's analyzer, HIGHLIGHT also gets each row's {@code _index}, so the row
 * is highlighted with the analyzer of the index it came from instead of {@code standard}. The key is a synthetic alias
 * of {@code _index} evaluated right above each relation and carried through every projection and every FORK or UNION ALL
 * branch up to HIGHLIGHT. A projection restores HIGHLIGHT's original output so the injected columns cannot affect later
 * commands, and a user {@code METADATA _index} that was renamed or dropped stays renamed or dropped. Rows that STATS and
 * ROW produce have no single source index, and DEDUP would group by the key, so those plans keep the {@code standard}
 * fallback and its warning.
 */
public class ResolveHighlightIndexKey extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    public static final String INDEX_KEY_NAME = Attribute.rawTemporaryName(MetadataAttribute.INDEX, "highlight");

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        if (context.minimumVersion().supports(TextEsField.TEXT_FIELD_ANALYZER) == false) {
            return plan; // an older node could not read the key or the mappings off the plan
        }
        return plan.transformUp(Highlight.class, highlight -> {
            if (highlight.indexKey() != null || highlight.resolved() == false || highlight.hasAnalyzerOption()) {
                return highlight; // WITH analyzer applies to every row
            }
            Map<String, TextEsField> mappings = mergedMappings(highlight);
            if (highlight.fields().stream().anyMatch(field -> HighlightAnalyzers.analyzerGroups(field, mappings) != null)) {
                LogicalPlan child = withIndexKey(highlight.child());
                if (child != null) {
                    // Keep execution-only columns below this boundary: DEDUP groups by every input column.
                    return new Project(
                        highlight.source(),
                        highlight.withIndexKeyAndMappings(child, indexKey(child), mappings),
                        highlight.output()
                    );
                }
            }
            return mappings.equals(highlight.fieldMappings())
                ? highlight
                : highlight.withIndexKeyAndMappings(highlight.child(), null, mappings);
        });
    }

    /** The mapping of each text ON column that comes unchanged out of a FORK or UNION ALL, by name. */
    private static Map<String, TextEsField> mergedMappings(Highlight highlight) {
        Map<String, TextEsField> mappings = new HashMap<>();
        for (NamedExpression field : highlight.fields()) {
            if (field instanceof Attribute column && (column instanceof FieldAttribute) == false && column.dataType() == TEXT) {
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
     * The mapping the branches of {@code merge} agree on for column {@code name}, or one naming the analyzer of each index
     * when branches that read different indices disagree. {@code null} when no branch maps the column, which is then
     * analyzed like any computed column, and a {@link UnknownAnalyzer#BRANCH_CONFLICT} when branches mix mapped and computed
     * values or disagree otherwise.
     */
    private static @Nullable TextEsField branchesMapping(MergePlan merge, String name) {
        record Mapped(LogicalPlan branch, Attribute column, TextEsField found) {}
        TextEsField conflict = mapping(name, null, TextEsField.DEFAULT_POSITION_INCREMENT_GAP, UnknownAnalyzer.BRANCH_CONFLICT, null);
        Set<String> computedAnalyzers = new HashSet<>();
        List<Mapped> mapped = new ArrayList<>();
        for (LogicalPlan branch : merge.children()) {
            Attribute column = firstNamed(branch.output(), name, a -> true);
            if (column == null || Fork.producesOnlyNull(branch, column)) {
                continue; // no values to analyze
            }
            TextEsField found = column instanceof FieldAttribute
                ? HighlightAnalyzers.mappingOf(column, Map.of())
                : mergedMapping(branch, column);
            if (found == null) {
                // Computed, so analyzed like any column without a mapping: with the analyzer it declares, or standard.
                String declared = AnalyzedTextExpression.valuesAnalyzerOf(column);
                computedAnalyzers.add(Objects.requireNonNullElse(declared, AnalyzedTextExpression.STANDARD_ANALYZER));
            } else {
                mapped.add(new Mapped(branch, column, found));
            }
        }
        if (mapped.isEmpty()) {
            return computedAnalyzers.size() > 1 ? conflict : null;
        }
        if (computedAnalyzers.isEmpty() == false) {
            return conflict;
        }
        List<TextEsField> mappings = mapped.stream().map(m -> mapping(name, m.found())).toList();
        if (mappings.stream().allMatch(mappings.getFirst()::equals)) {
            return mappings.getFirst();
        }
        // Each row comes from one branch, so it can still use the analyzer of the index it was read from.
        List<IndexAnalyzerGroup> groups = new ArrayList<>();
        for (Mapped m : mapped) {
            List<IndexAnalyzerGroup> branchGroups = indexGroups(m.branch(), m.column(), m.found());
            if (branchGroups == null) {
                return conflict;
            }
            groups.addAll(branchGroups);
        }
        List<IndexAnalyzerGroup> perIndex = byAnalyzer(groups);
        int gap = mappings.getFirst().positionIncrementGap();
        return perIndex == null ? conflict : mapping(name, null, gap, UnknownAnalyzer.CONFLICT, perIndex);
    }

    /**
     * Which indices of {@code branch} analyze {@code column}, mapped as {@code found}, with which analyzer, or {@code null}
     * when the column is not read off the relation the branch's rows come from, like a LOOKUP JOIN field.
     */
    private static @Nullable List<IndexAnalyzerGroup> indexGroups(LogicalPlan branch, Attribute column, TextEsField found) {
        if (found.analyzerGroups() != null) {
            return found.analyzerGroups();
        }
        EsRelation relation = rowSource(branch);
        if (relation == null || relation.outputSet().contains(column) == false) {
            return null;
        }
        return switch (found.unknownAnalyzer()) {
            case NONE, INDEX_LOCAL, NOT_REPORTED -> List.of(
                new IndexAnalyzerGroup(
                    found.analyzerName(),
                    found.unknownAnalyzer() == UnknownAnalyzer.INDEX_LOCAL,
                    found.positionIncrementGap(),
                    relation.concreteQualifiedIndices()
                )
            );
            case CONFLICT, BRANCH_CONFLICT -> null; // disagreement below that names no indices
        };
    }

    /** The relation the rows of {@code plan} come from: the one {@link #withIndexKey} reads their {@code _index} off. */
    private static @Nullable EsRelation rowSource(LogicalPlan plan) {
        return switch (plan) {
            case EsRelation relation -> relation;
            case LeafPlan ignored -> null;
            case UnaryPlan unary -> rowSource(unary.child());
            case BinaryPlan binary -> rowSource(binary.left());
            case MergePlan ignored -> null; // rows from several relations
            default -> throw new IllegalStateException("unexpected plan [" + plan.nodeName() + "] under HIGHLIGHT");
        };
    }

    /** {@code groups} of several branches as one per analyzer, or {@code null} when two name different analyzers for an index. */
    private static @Nullable List<IndexAnalyzerGroup> byAnalyzer(List<IndexAnalyzerGroup> groups) {
        record Analyzer(@Nullable String name, boolean indexLocal, int positionIncrementGap) {}
        Map<String, Analyzer> analyzerByIndex = new TreeMap<>();
        for (IndexAnalyzerGroup group : groups) {
            Analyzer analyzer = new Analyzer(group.analyzerName(), group.indexLocal(), group.positionIncrementGap());
            for (String index : group.indices()) {
                Analyzer previous = analyzerByIndex.putIfAbsent(index, analyzer);
                if (previous != null && previous.equals(analyzer) == false) {
                    return null;
                }
            }
        }
        Map<Analyzer, Set<String>> indicesByAnalyzer = new LinkedHashMap<>();
        analyzerByIndex.forEach((index, analyzer) -> indicesByAnalyzer.computeIfAbsent(analyzer, k -> new TreeSet<>()).add(index));
        return indicesByAnalyzer.entrySet()
            .stream()
            .map(e -> new IndexAnalyzerGroup(e.getKey().name(), e.getKey().indexLocal(), e.getKey().positionIncrementGap(), e.getValue()))
            .toList();
    }

    private static TextEsField mapping(String name, TextEsField found) {
        return mapping(name, found.analyzerName(), found.positionIncrementGap(), found.unknownAnalyzer(), found.analyzerGroups());
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

    /** Returns {@code plan} with the key in its output, or {@code null} when its rows have no single source index. */
    private static LogicalPlan withIndexKey(LogicalPlan plan) {
        // Reuse the key an earlier HIGHLIGHT carries up: a second alias of the same name would shadow it in Eval's output.
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
            case LeafPlan ignored -> null; // ROW, LocalRelation, ExternalRelation: no source index per row
            case Project project -> {
                LogicalPlan child = withIndexKey(project.child());
                if (child == null) {
                    yield null;
                }
                List<NamedExpression> projections = CollectionUtils.combine(project.projections(), indexKey(child));
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
            case MergePlan merge -> {
                List<LogicalPlan> branches = new ArrayList<>(merge.children().size());
                for (LogicalPlan branch : merge.children()) {
                    LogicalPlan withKey = withIndexKey(branch);
                    // Branches line up by position, so the key must come last in each, as it does in the merge output.
                    if (withKey == null || withKey.output().getLast().equals(indexKey(withKey)) == false) {
                        yield null;
                    }
                    branches.add(withKey);
                }
                yield merge.replaceSubPlans(branches).refreshOutput();
            }
            default -> throw new IllegalStateException("unexpected plan [" + plan.nodeName() + "] under HIGHLIGHT");
        };
        // Aggregations drop the key from the output.
        return result != null && indexKey(result) != null ? result : null;
    }

    /** The key in {@code plan}'s output: only this rule makes a synthetic column of that name. */
    private static @Nullable Attribute indexKey(LogicalPlan plan) {
        return firstNamed(plan.output(), INDEX_KEY_NAME, Attribute::synthetic);
    }

    private static Attribute firstNamed(List<Attribute> attributes, String name, Predicate<Attribute> filter) {
        return attributes.stream().filter(a -> a.name().equals(name) && filter.test(a)).findFirst().orElse(null);
    }
}
