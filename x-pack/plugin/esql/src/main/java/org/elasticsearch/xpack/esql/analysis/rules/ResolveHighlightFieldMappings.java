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
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.IndexAnalyzerGroup;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField.UnknownAnalyzer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.highlight.HighlightAnalyzers;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.analysis.rules.ResolveHighlightIndexKey.rowSourceOf;

/**
 * Gives HIGHLIGHT back the mapping of text ON columns that {@code RENAME}, a plain {@code EVAL} copy, a
 * {@code STATS BY} alias, an unchanged {@code FORK} or {@code UNION ALL}, or {@code FUSE} turned into a
 * {@link ReferenceAttribute}. A copy keeps the field's mapping, so it is analyzed the same way as the field. A merged
 * column gets one of these mappings:
 * <ul>
 *     <li>the mapping every branch agrees on;</li>
 *     <li>a mapping that names each index's analyzer, when branches over different indices disagree;</li>
 *     <li>a {@link UnknownAnalyzer#BRANCH_CONFLICT}, which fails the query unless {@code WITH} picks an analyzer.</li>
 * </ul>
 * An expression over a field has no mapping, and neither does a column no branch maps. HIGHLIGHT analyzes those like
 * any other computed column, except that a column FUSE read from a {@code TO_TEXT} column gets a mapping that names
 * the analyzer {@code TO_TEXT} declares, because FUSE drops the declaration.
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

    /**
     * Mapping of each ON column that isn't a field attribute but reads a text field, by name. FUSE returns a text column as
     * {@code keyword}, so keyword columns are followed too.
     */
    private static Map<String, TextEsField> mergedMappings(Highlight highlight) {
        Lineage lineage = new Lineage();
        Map<String, TextEsField> mappings = new HashMap<>();
        for (NamedExpression field : highlight.fields()) {
            if (field instanceof Attribute column && (column instanceof FieldAttribute) == false && DataType.isString(column.dataType())) {
                TextEsField mapping = mergedMapping(highlight.child(), column, lineage);
                if (mapping == null) {
                    mapping = droppedDeclaration(highlight.child(), column, lineage);
                }
                if (mapping != null) {
                    mappings.put(column.name(), mapping(column.name(), mapping));
                }
            }
        }
        return Map.copyOf(mappings);
    }

    /**
     * Mapping of {@code column} when a {@code RENAME} or {@code EVAL} copy still reads a mapped field, or the column
     * came out of a {@code FORK} or {@code UNION ALL}, directly or through {@code FUSE}.
     */
    private static @Nullable TextEsField mergedMapping(LogicalPlan plan, Attribute column, Lineage lineage) {
        Expression read = lineage.aliases(plan).resolve(column, column);
        if (read instanceof FieldAttribute field) {
            return HighlightAnalyzers.mappingOf(field, Map.of());
        }
        // A merge gives its output new ids, so resolving aliases stops on the merged column.
        if (read instanceof Attribute merged && rowSourceOf(plan, merged) instanceof MergePlan merge) {
            return branchesMapping(merge, merged.name(), lineage);
        }
        return null; // an expression, not a field
    }

    /**
     * A mapping that names the analyzer TO_TEXT declares for the column {@code column} reads, when {@code column} itself
     * declares none. FUSE reads each column through FIRST, which returns {@code keyword} and so drops the declaration.
     */
    private static @Nullable TextEsField droppedDeclaration(LogicalPlan plan, Attribute column, Lineage lineage) {
        if (AnalyzedTextExpression.valuesAnalyzerOf(column) != null) {
            return null;
        }
        String declared = AnalyzedTextExpression.valuesAnalyzerOf(lineage.aliases(plan).resolve(column, column));
        return declared == null
            ? null
            : mapping(column.name(), declared, TextEsField.DEFAULT_POSITION_INCREMENT_GAP, UnknownAnalyzer.NONE, null);
    }

    /**
     * The mapping of column {@code name} across the branches of {@code merge}:
     * <ul>
     *     <li>the mapping every branch agrees on;</li>
     *     <li>a mapping that names each index's analyzer, when branches that read different indices disagree;</li>
     *     <li>{@code null} when no branch maps the column and the branches that compute it agree on its analyzer, so
     *     HIGHLIGHT analyzes it like any computed column;</li>
     *     <li>a {@link UnknownAnalyzer#BRANCH_CONFLICT} when a computed branch uses a different analyzer from the mapped
     *     branches, or the branches disagree in any other way.</li>
     * </ul>
     */
    private static @Nullable TextEsField branchesMapping(MergePlan merge, String name, Lineage lineage) {
        Set<String> computedAnalyzers = new HashSet<>();
        List<BranchColumn> mapped = new ArrayList<>();
        for (BranchColumn b : branchColumns(merge, name, lineage)) {
            if (b.found() == null) {
                // The branch computes the column, so HIGHLIGHT uses the analyzer the column declares, or standard.
                String declared = AnalyzedTextExpression.valuesAnalyzerOf(b.column());
                computedAnalyzers.add(Objects.requireNonNullElse(declared, AnalyzedTextExpression.STANDARD_ANALYZER));
            } else {
                mapped.add(b);
            }
        }
        if (mapped.isEmpty()) {
            return computedAnalyzers.size() > 1 ? branchConflict(name) : null;
        }
        List<TextEsField> distinct = mapped.stream().map(m -> mapping(name, m.found())).distinct().toList();
        if (computedAnalyzers.isEmpty() == false) {
            // Computed rows use the analyzer they declare, so they only agree with one mapping that names that analyzer.
            return distinct.size() == 1 && analyzesLike(distinct.getFirst(), computedAnalyzers)
                ? distinct.getFirst()
                : branchConflict(name);
        }
        if (distinct.size() == 1 && distinct.getFirst().analyzerGroups() == null) {
            return distinct.getFirst();
        }
        // Each row comes from one branch, so it can still use the analyzer of the index it was read from.
        List<IndexAnalyzerGroup> perIndex = indexGroups(mapped, lineage);
        if (perIndex == null && distinct.size() > 1) {
            return branchConflict(name);
        }
        // Agreed groups the key cannot route, like a LOOKUP JOIN field's, become a conflict that names no indices and fails.
        return mapping(name, null, TextEsField.DEFAULT_POSITION_INCREMENT_GAP, UnknownAnalyzer.CONFLICT, perIndex);
    }

    /** A declared analyzer resolves with the default gap, so only a mapping with that gap analyzes values the same way. */
    private static boolean analyzesLike(TextEsField mapping, Set<String> computedAnalyzers) {
        return mapping.unknownAnalyzer() == UnknownAnalyzer.NONE
            && mapping.positionIncrementGap() == TextEsField.DEFAULT_POSITION_INCREMENT_GAP
            && computedAnalyzers.equals(Set.of(mapping.analyzerName()));
    }

    private static TextEsField branchConflict(String name) {
        return mapping(name, null, TextEsField.DEFAULT_POSITION_INCREMENT_GAP, UnknownAnalyzer.BRANCH_CONFLICT, null);
    }

    /** A branch's column of a given name. {@code found} is its mapping, or {@code null} when the branch computes the column. */
    private record BranchColumn(LogicalPlan branch, Attribute column, @Nullable TextEsField found) {}

    private static List<BranchColumn> branchColumns(MergePlan merge, String name, Lineage lineage) {
        List<BranchColumn> columns = new ArrayList<>();
        List<Map<String, Attribute>> valued = lineage.branchOutputs(merge);
        for (int i = 0; i < valued.size(); i++) {
            Attribute column = valued.get(i).get(name);
            if (column != null) {
                LogicalPlan branch = merge.children().get(i);
                columns.add(new BranchColumn(branch, column, mergedMapping(branch, column, lineage)));
            }
        }
        return columns;
    }

    /**
     * Alias bindings, and the columns each branch actually has values for. Both are cached, because resolving every ON
     * column through every branch is quadratic in how wide the indices are.
     */
    private static final class Lineage {
        private final Map<LogicalPlan, AttributeMap<Expression>> aliasesByPlan = new IdentityHashMap<>();
        private final Map<MergePlan, List<Map<String, Attribute>>> branchOutputs = new IdentityHashMap<>();

        /** Bindings for {@code plan}. A branch has to use its own, or a column can resolve into another branch. */
        AttributeMap<Expression> aliases(LogicalPlan plan) {
            return aliasesByPlan.computeIfAbsent(plan, ResolveHighlightIndexKey::aliases);
        }

        /** One map per branch of {@code merge}, in branch order, without the columns the branch fills with nulls. */
        List<Map<String, Attribute>> branchOutputs(MergePlan merge) {
            return branchOutputs.computeIfAbsent(merge, m -> m.children().stream().map(Lineage::valuedColumns).toList());
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
    private static @Nullable List<IndexAnalyzerGroup> indexGroups(List<BranchColumn> branches, Lineage lineage) {
        Map<IndexField, Set<IndexAnalyzerGroup.Analyzer>> claims = new HashMap<>();
        return addIndexGroups(branches, lineage, claims) ? byAnalyzer(claims) : null;
    }

    /** A source field as one index maps it. */
    private record IndexField(String index, FieldAttribute.FieldName field) {}

    /**
     * Adds the analyzer that each branch gives each of its indices for the field the branch reads, through nested merges.
     * Returns {@code false} when a branch computes the column, when the column does not come from the plan that produces
     * the branch's rows, like a LOOKUP JOIN field, or when the mapping is a conflict that names no indices.
     */
    private static boolean addIndexGroups(
        List<BranchColumn> branches,
        Lineage lineage,
        Map<IndexField, Set<IndexAnalyzerGroup.Analyzer>> claims
    ) {
        for (BranchColumn b : branches) {
            Expression read = lineage.aliases(b.branch()).resolve(b.column(), b.column());
            LogicalPlan source = b.found() == null ? null : rowSourceOf(b.branch(), read);
            if (source instanceof MergePlan nested) {
                // A nested merge's branches may agree on the mapping, which then names no indices.
                if (addIndexGroups(branchColumns(nested, Expressions.name(read), lineage), lineage, claims) == false) {
                    return false;
                }
            } else if (source instanceof EsRelation relation && read instanceof FieldAttribute field) {
                List<IndexAnalyzerGroup> groups = relationGroups(b.found(), relation);
                if (groups == null) {
                    return false;
                }
                for (IndexAnalyzerGroup group : groups) {
                    for (String index : group.indices()) {
                        claims.computeIfAbsent(new IndexField(index, field.fieldName()), k -> new HashSet<>()).add(group.analyzer());
                    }
                }
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * The groups of a column read from {@code relation}. A mapping that names no groups gets one group with every index
     * of the relation, including indices that do not map the field. Returns {@code null} for a conflict that names no
     * indices.
     */
    private static @Nullable List<IndexAnalyzerGroup> relationGroups(TextEsField found, EsRelation relation) {
        if (found.analyzerGroups() != null) {
            return found.analyzerGroups();
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

    /**
     * Picks one analyzer per index and groups indices that use the same one.
     * <p>
     * A relation copies its analyzer onto every index it reads, even indices without this field. An index that has
     * the field reports the same name every time. Two analyzers on one index, one of them named, means the index
     * does not have the field: the values are {@code null}, so the index is left out.
     * <p>
     * Returns {@code null} if both analyzers have no name, or if two fields of one index use different analyzers.
     * Those rows may hold the field, so neither analyzer can be dropped. Both can lack a name because a relation
     * marks every index index-local when any one of them is, including an index whose analyzer was not reported.
     */
    private static @Nullable List<IndexAnalyzerGroup> byAnalyzer(Map<IndexField, Set<IndexAnalyzerGroup.Analyzer>> claims) {
        Map<String, IndexAnalyzerGroup.Analyzer> analyzerByIndex = new TreeMap<>();
        for (Map.Entry<IndexField, Set<IndexAnalyzerGroup.Analyzer>> claim : claims.entrySet()) {
            Set<IndexAnalyzerGroup.Analyzer> analyzers = claim.getValue();
            if (analyzers.size() > 1) {
                if (analyzers.stream().allMatch(analyzer -> analyzer.name() == null)) {
                    return null;
                }
                continue;
            }
            IndexAnalyzerGroup.Analyzer analyzer = analyzers.iterator().next();
            IndexAnalyzerGroup.Analyzer previous = analyzerByIndex.putIfAbsent(claim.getKey().index(), analyzer);
            if (previous != null && previous.equals(analyzer) == false) {
                return null; // two source fields read from one index use different analyzers
            }
        }
        return IndexAnalyzerGroup.byAnalyzer(analyzerByIndex);
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
