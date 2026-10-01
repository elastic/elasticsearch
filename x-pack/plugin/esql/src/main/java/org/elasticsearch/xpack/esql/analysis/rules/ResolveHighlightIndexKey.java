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
import java.util.IdentityHashMap;
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
 * Gives HIGHLIGHT the mapping and the source index it needs to pick each row's mapping analyzer, when the plan between
 * the relation and HIGHLIGHT drops them.
 * <p>
 * FORK and UNION ALL output each merged column as a {@link ReferenceAttribute}, which has no mapping. For each such text
 * ON column, this rule gives HIGHLIGHT one of these mappings:
 * <ul>
 *     <li>the mapping every branch agrees on;</li>
 *     <li>a mapping that names each index's analyzer, when branches over different indices disagree;</li>
 *     <li>a {@link UnknownAnalyzer#BRANCH_CONFLICT}, which falls back to {@code standard} with a warning.</li>
 * </ul>
 * A column no branch maps gets no mapping, and HIGHLIGHT analyzes it like any computed column.
 * <p>
 * When the queried indices disagree on an ON field's analyzer, this rule also passes HIGHLIGHT each row's {@code _index},
 * so HIGHLIGHT uses the analyzer of the row's index instead of {@code standard}. The key is a synthetic alias of
 * {@code _index}. This rule evaluates it right above each relation and adds it to every projection and every FORK or
 * UNION ALL branch up to HIGHLIGHT. A projection above HIGHLIGHT restores HIGHLIGHT's original output, so later commands
 * never see the added columns. A user {@code METADATA _index} that was renamed or dropped stays renamed or dropped.
 * <p>
 * STATS and ROW rows have no single source index, and DEDUP would group by the key. Those plans keep the
 * {@code standard} fallback and its warning.
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
                    // Project away the key and any added _index, which a later DEDUP would group by.
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
        BranchOutputs outputs = new BranchOutputs();
        for (NamedExpression field : highlight.fields()) {
            if (field instanceof Attribute column && (column instanceof FieldAttribute) == false && column.dataType() == TEXT) {
                TextEsField mapping = mergedMapping(highlight.child(), column, outputs);
                if (mapping != null) {
                    mappings.put(column.name(), mapping);
                }
            }
        }
        return Map.copyOf(mappings);
    }

    /** The mapping of {@code column}, an output of {@code plan}, if the column comes unchanged out of a FORK or UNION ALL. */
    private static @Nullable TextEsField mergedMapping(LogicalPlan plan, Attribute column, BranchOutputs outputs) {
        if (plan instanceof MergePlan merge) {
            return branchesMapping(merge, column.name(), outputs);
        }
        for (LogicalPlan child : plan.children()) {
            if (child.outputSet().contains(column)) {
                return mergedMapping(child, column, outputs);
            }
        }
        return null; // computed, e.g. by EVAL or RENAME
    }

    /**
     * The mapping of column {@code name} across the branches of {@code merge}:
     * <ul>
     *     <li>the mapping every branch agrees on;</li>
     *     <li>a mapping that names each index's analyzer, when branches that read different indices disagree;</li>
     *     <li>{@code null} when no branch maps the column and the branches that compute it agree on its analyzer, so
     *     HIGHLIGHT analyzes it like any computed column;</li>
     *     <li>a {@link UnknownAnalyzer#BRANCH_CONFLICT} when branches mix mapped and computed values, or disagree in any
     *     other way.</li>
     * </ul>
     */
    private static @Nullable TextEsField branchesMapping(MergePlan merge, String name, BranchOutputs outputs) {
        TextEsField conflict = mapping(name, null, TextEsField.DEFAULT_POSITION_INCREMENT_GAP, UnknownAnalyzer.BRANCH_CONFLICT, null);
        Set<String> computedAnalyzers = new HashSet<>();
        List<BranchColumn> mapped = new ArrayList<>();
        for (BranchColumn b : branchColumns(merge, name, outputs)) {
            if (b.found() == null) {
                // The branch computes the column, so HIGHLIGHT uses the analyzer the column declares, or standard.
                String declared = AnalyzedTextExpression.valuesAnalyzerOf(b.column());
                computedAnalyzers.add(Objects.requireNonNullElse(declared, AnalyzedTextExpression.STANDARD_ANALYZER));
            } else {
                mapped.add(b);
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
        List<IndexAnalyzerGroup> perIndex = indexGroups(mapped, outputs);
        int gap = mappings.getFirst().positionIncrementGap();
        return perIndex == null ? conflict : mapping(name, null, gap, UnknownAnalyzer.CONFLICT, perIndex);
    }

    /** A branch's column of a given name. {@code found} is its mapping, or {@code null} when the branch computes the column. */
    private record BranchColumn(LogicalPlan branch, Attribute column, @Nullable TextEsField found) {}

    private static List<BranchColumn> branchColumns(MergePlan merge, String name, BranchOutputs outputs) {
        List<BranchColumn> columns = new ArrayList<>();
        List<Map<String, Attribute>> valued = outputs.of(merge);
        for (int i = 0; i < valued.size(); i++) {
            Attribute column = valued.get(i).get(name);
            if (column != null) {
                LogicalPlan branch = merge.children().get(i);
                TextEsField found = column instanceof FieldAttribute
                    ? HighlightAnalyzers.mappingOf(column, Map.of())
                    : mergedMapping(branch, column, outputs);
                columns.add(new BranchColumn(branch, column, found));
            }
        }
        return columns;
    }

    /**
     * The columns each branch of a FORK or UNION ALL has values of, by name. Each merge is indexed once, because looking up
     * every ON column in every branch output is quadratic in the number of columns of the queried indices.
     */
    private static final class BranchOutputs {
        private final Map<MergePlan, List<Map<String, Attribute>>> byMerge = new IdentityHashMap<>();

        /** One map per branch of {@code merge}, in branch order, without the columns the branch fills with nulls. */
        List<Map<String, Attribute>> of(MergePlan merge) {
            return byMerge.computeIfAbsent(merge, m -> m.children().stream().map(BranchOutputs::valuedColumns).toList());
        }

        private static Map<String, Attribute> valuedColumns(LogicalPlan branch) {
            Predicate<Attribute> onlyNull = Fork.producesOnlyNull(branch);
            Map<String, Attribute> columns = new HashMap<>();
            for (Attribute column : branch.output()) {
                columns.putIfAbsent(column.name(), column);
            }
            columns.values().removeIf(onlyNull);
            return columns;
        }
    }

    /**
     * The analyzer of each index that {@code branches} read rows from. {@code null} when a branch cannot name its indices,
     * or when two branches give one index different analyzers.
     */
    private static @Nullable List<IndexAnalyzerGroup> indexGroups(List<BranchColumn> branches, BranchOutputs outputs) {
        List<IndexAnalyzerGroup> groups = new ArrayList<>();
        for (BranchColumn b : branches) {
            List<IndexAnalyzerGroup> branchGroups = b.found() == null ? null : indexGroups(b.branch(), b.column(), b.found(), outputs);
            if (branchGroups == null) {
                return null;
            }
            groups.addAll(branchGroups);
        }
        return byAnalyzer(groups);
    }

    /**
     * The analyzer each index of {@code branch} uses for {@code column}, whose mapping is {@code found}. {@code null} when
     * the column does not come from the plan that produces the branch's rows, like a LOOKUP JOIN field, or when the
     * mapping is a conflict that names no indices.
     */
    private static @Nullable List<IndexAnalyzerGroup> indexGroups(
        LogicalPlan branch,
        Attribute column,
        TextEsField found,
        BranchOutputs outputs
    ) {
        if (found.analyzerGroups() != null) {
            return found.analyzerGroups();
        }
        LogicalPlan source = rowSource(branch);
        if (source == null || source.outputSet().contains(column) == false) {
            return null;
        }
        if (source instanceof MergePlan nested) {
            // A nested merge's branches may agree on the mapping, which then names no indices.
            return indexGroups(branchColumns(nested, column.name(), outputs), outputs);
        }
        return switch (found.unknownAnalyzer()) {
            case NONE, INDEX_LOCAL, NOT_REPORTED -> List.of(
                new IndexAnalyzerGroup(
                    found.analyzerName(),
                    found.unknownAnalyzer() == UnknownAnalyzer.INDEX_LOCAL,
                    found.positionIncrementGap(),
                    ((EsRelation) source).concreteQualifiedIndices()
                )
            );
            case CONFLICT, BRANCH_CONFLICT -> null; // disagreement below that names no indices
        };
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

    /**
     * Merges the {@code groups} of several branches into one group per analyzer. {@code null} when two groups give one index
     * different analyzers.
     */
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

    /** A mapping with only what picks the analyzer. Branches that agree on the analyzer may still differ on sub-fields or doc values. */
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

    /** The key in {@code plan}'s output. Matching by name is safe because only this rule makes a synthetic column of that name. */
    private static @Nullable Attribute indexKey(LogicalPlan plan) {
        return firstNamed(plan.output(), INDEX_KEY_NAME, Attribute::synthetic);
    }

    private static Attribute firstNamed(List<Attribute> attributes, String name, Predicate<Attribute> filter) {
        return attributes.stream().filter(a -> a.name().equals(name) && filter.test(a)).findFirst().orElse(null);
    }
}
