/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.IndexAnalyzerGroup;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField.UnknownAnalyzer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
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

import static org.elasticsearch.xpack.esql.analysis.rules.ResolveHighlightIndexKey.beforeRenames;
import static org.elasticsearch.xpack.esql.analysis.rules.ResolveHighlightIndexKey.renamedBy;
import static org.elasticsearch.xpack.esql.analysis.rules.ResolveHighlightIndexKey.rowSourceOf;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Gives HIGHLIGHT the mapping of each text ON column that RENAME renamed from a mapped field, or that comes unchanged out
 * of a FORK or UNION ALL. Those commands output the column as a {@link ReferenceAttribute}, which has no mapping. A renamed
 * field gets its own mapping, so it is analyzed exactly as the field is. A merged column gets one of these mappings:
 * <ul>
 *     <li>the mapping every branch agrees on;</li>
 *     <li>a mapping that names each index's analyzer, when branches over different indices disagree;</li>
 *     <li>a {@link UnknownAnalyzer#BRANCH_CONFLICT}, which falls back to {@code standard} with a warning.</li>
 * </ul>
 * A column EVAL copies is a new column with no mapping, and so is a column no branch maps. HIGHLIGHT analyzes those like
 * any computed column.
 * <p>
 * Runs before {@link ResolveHighlightIndexKey}, which threads each row's {@code _index} through every branch when a mapping
 * names each index's analyzer.
 */
public class ResolveHighlightFieldMappings extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        if (context.minimumVersion().supports(TextEsField.TEXT_FIELD_ANALYZER) == false) {
            return plan; // an older node could not read the mappings off the plan
        }
        return plan.transformUp(Highlight.class, highlight -> {
            if (highlight.resolved() == false || highlight.hasAnalyzerOption()) {
                return highlight; // WITH analyzer applies to every row
            }
            Map<String, TextEsField> mappings = mergedMappings(highlight);
            return mappings.equals(highlight.fieldMappings()) ? highlight : highlight.withFieldMappings(mappings);
        });
    }

    /** The mapping of each text ON column that renames a mapped field or comes unchanged out of a FORK or UNION ALL, by name. */
    private static Map<String, TextEsField> mergedMappings(Highlight highlight) {
        Map<String, TextEsField> mappings = new HashMap<>();
        BranchOutputs outputs = new BranchOutputs();
        for (NamedExpression field : highlight.fields()) {
            if (field instanceof Attribute column && (column instanceof FieldAttribute) == false && column.dataType() == TEXT) {
                TextEsField mapping = mergedMapping(highlight.child(), column, outputs);
                if (mapping != null) {
                    mappings.put(column.name(), mapping(column.name(), mapping));
                }
            }
        }
        return Map.copyOf(mappings);
    }

    /**
     * The mapping of {@code column}, an output of {@code plan}, if the column is a mapped field, renames one, or comes
     * unchanged out of a FORK or UNION ALL.
     */
    private static @Nullable TextEsField mergedMapping(LogicalPlan plan, Attribute column, BranchOutputs outputs) {
        if (column instanceof FieldAttribute) {
            return HighlightAnalyzers.mappingOf(column, Map.of());
        }
        if (plan instanceof MergePlan merge) {
            return branchesMapping(merge, column.name(), outputs);
        }
        if (plan instanceof Project project) {
            Attribute renamed = renamedBy(project, column);
            if (renamed != null) {
                return mergedMapping(project.child(), renamed, outputs);
            }
        }
        for (LogicalPlan child : plan.children()) {
            if (child.outputSet().contains(column)) {
                return mergedMapping(child, column, outputs);
            }
        }
        return null; // computed, e.g. by EVAL
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
        List<TextEsField> distinct = mapped.stream().map(m -> mapping(name, m.found())).distinct().toList();
        boolean agreed = distinct.size() == 1;
        if (agreed && distinct.getFirst().analyzerGroups() == null) {
            return distinct.getFirst();
        }
        // Each row comes from one branch, so it can still use the analyzer of the index it was read from.
        List<IndexAnalyzerGroup> perIndex = indexGroups(mapped, outputs);
        // Agreed groups the key cannot route, like a LOOKUP JOIN field's, keep the warning that the indices disagree.
        return perIndex == null && agreed == false
            ? conflict
            : mapping(name, null, TextEsField.DEFAULT_POSITION_INCREMENT_GAP, UnknownAnalyzer.CONFLICT, perIndex);
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
                columns.add(new BranchColumn(branch, column, mergedMapping(branch, column, outputs)));
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
            List<IndexAnalyzerGroup> branchGroups = indexGroups(b, outputs);
            if (branchGroups == null) {
                return null;
            }
            groups.addAll(branchGroups);
        }
        return byAnalyzer(groups);
    }

    /**
     * The analyzer each index of the branch uses for its column. {@code null} when the branch computes the column, when
     * the column does not come from the plan that produces the branch's rows, like a LOOKUP JOIN field, or when the
     * mapping is a conflict that names no indices.
     */
    private static @Nullable List<IndexAnalyzerGroup> indexGroups(BranchColumn b, BranchOutputs outputs) {
        TextEsField found = b.found();
        LogicalPlan source = found == null ? null : rowSourceOf(b.branch(), b.column());
        if (source == null) {
            return null;
        }
        if (found.analyzerGroups() != null) {
            return found.analyzerGroups();
        }
        if (source instanceof MergePlan nested) {
            // A nested merge's branches may agree on the mapping, which then names no indices.
            return indexGroups(branchColumns(nested, beforeRenames(b.branch(), b.column()).name(), outputs), outputs);
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
}
