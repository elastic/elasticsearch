/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.highlight;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.function.fulltext.SingleFieldFullTextFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.plan.logical.DocPreserving;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.planner.HighlightQueryBuilders;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.query.MatchQueryBuilder.ANALYZER_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.QUOTE_ANALYZER_FIELD;

/** Analysis-time helpers for implicit HIGHLIGHT query and field lists. */
public final class HighlightSupport {

    private HighlightSupport() {}

    /**
     * Returns whether a {@code WHERE} conjunct can be borrowed for highlighting. Positive full-text predicates and
     * boolean combinations of them are supported; negative and mixed full-text/non-full-text predicates are not.
     * Every accepted expression must also be supported by
     * {@link org.elasticsearch.xpack.esql.planner.HighlightQueryBuilders#build}.
     */
    public static boolean isSupportedImplicitPredicate(Expression expr) {
        if (expr instanceof BinaryLogic binary) {
            return isSupportedImplicitPredicate(binary.left()) && isSupportedImplicitPredicate(binary.right());
        }
        return isBorrowableFullText(expr);
    }

    private static boolean isBorrowableFullText(Expression expr) {
        return expr instanceof Match || expr instanceof MatchPhrase || expr instanceof QueryString || expr instanceof Kql;
    }

    /** The leaf's {@code analyzer} option, or {@code null} if absent, not foldable, or unsupported on that leaf type. */
    private static String analyzerNameOf(Expression fullTextLeaf) {
        Expression options = switch (fullTextLeaf) {
            case SingleFieldFullTextFunction single -> single.options();
            case QueryString queryString -> queryString.options();
            case Kql kql -> null;
            default -> throw new IllegalStateException(
                "analyzerNameOf: unexpected full-text leaf [" + fullTextLeaf.getClass().getSimpleName() + "]"
            );
        };
        return foldedOption(options, ANALYZER_FIELD.getPreferredName());
    }

    private static String quoteAnalyzerNameOf(Expression fullTextLeaf) {
        if (fullTextLeaf instanceof QueryString queryString) {
            return foldedOption(queryString.options(), QUOTE_ANALYZER_FIELD.getPreferredName());
        }
        return null;
    }

    /** The folded string value of option {@code name} in {@code options}, or {@code null} if absent or not a foldable constant. */
    private static String foldedOption(Expression options, String name) {
        if (options instanceof MapExpression map) {
            Expression value = map.get(name);
            if (value != null && value.foldable()) {
                return BytesRefs.toString(value.fold(FoldContext.small()));
            }
        }
        return null;
    }

    /**
     * The one non-default analyzer every full-text leaf agrees on, or {@code null} if they disagree, name none, or
     * all use {@code standard}. An unlabeled leaf counts as {@code standard}, so it disagrees with a labeled sibling
     * instead of inheriting that sibling's analyzer.
     */
    public static String uniformAnalyzerOf(Expression query) {
        Set<String> names = new LinkedHashSet<>();
        query.forEachDown(FullTextFunction.class, leaf -> {
            String analyzer = analyzerNameOf(leaf);
            names.add(analyzer == null ? HighlightQueryBuilders.DEFAULT_ANALYZER_NAME : analyzer);
        });
        if (names.size() != 1) {
            return null;
        }
        String only = names.iterator().next();
        return only.equals(HighlightQueryBuilders.DEFAULT_ANALYZER_NAME) ? null : only;
    }

    /**
     * Analyzer names a full-text leaf asks the runtime context to resolve: each leaf's {@code analyzer} option and
     * any {@code QSTR} {@code quote_analyzer}. Includes names on leaves outside ON, because query builders validate
     * the option before the (lenient) field lookup.
     */
    public static Set<String> leafAnalyzerNamesOf(Expression query) {
        Set<String> names = new LinkedHashSet<>();
        query.forEachDown(FullTextFunction.class, leaf -> {
            addIfPresent(names, analyzerNameOf(leaf));
            addIfPresent(names, quoteAnalyzerNameOf(leaf));
        });
        return names;
    }

    private static void addIfPresent(Set<String> names, String name) {
        if (name != null) {
            names.add(name);
        }
    }

    /**
     * Analyzer used to highlight each ON field. Non-null {@code commandAnalyzerName} applies to every field.
     * Otherwise a leaf's effective analyzer applies to the field it names ({@code QSTR}/{@code KQL}: every ON
     * field); see {@link #effectiveFieldAnalyzerName}. Leaves with no effective analyzer are skipped; fields no
     * leaf labels default to {@code standard}.
     *
     * @throws IllegalArgumentException if two leaves name different analyzers for one field
     */
    public static Map<String, String> fieldAnalyzers(Expression query, @Nullable String commandAnalyzerName, List<String> onFields) {
        Map<String, String> result = new LinkedHashMap<>();
        if (commandAnalyzerName != null) {
            for (String field : onFields) {
                result.put(field, commandAnalyzerName);
            }
            return result;
        }
        Map<String, Set<String>> assignedByField = new LinkedHashMap<>();
        for (String field : onFields) {
            assignedByField.put(field, new LinkedHashSet<>());
        }
        query.forEachDown(FullTextFunction.class, leaf -> assignLeafAnalyzer(assignedByField, leaf));
        for (String field : onFields) {
            result.put(field, uniqueAnalyzer(field, assignedByField.get(field)));
        }
        return result;
    }

    private static void assignLeafAnalyzer(Map<String, Set<String>> assignedByField, Expression leaf) {
        String analyzer = effectiveFieldAnalyzerName(leaf);
        if (analyzer == null) {
            return;
        }
        String single = leafFieldName(leaf);
        if (single != null) {
            Set<String> names = assignedByField.get(single);
            if (names != null) {
                names.add(analyzer);
            }
            return;
        }
        for (Set<String> names : assignedByField.values()) {
            names.add(analyzer);
        }
    }

    private static String uniqueAnalyzer(String field, Set<String> names) {
        if (names.size() > 1) {
            throw new IllegalArgumentException(
                "HIGHLIGHT field ["
                    + field
                    + "] would be highlighted with different analyzers "
                    + names
                    + " by different clauses of the query; use the same analyzer for every clause on that field"
            );
        }
        return names.isEmpty() ? HighlightQueryBuilders.DEFAULT_ANALYZER_NAME : names.iterator().next();
    }

    private static String leafFieldName(Expression fullTextLeaf) {
        Expression field = leafField(fullTextLeaf);
        return field == null ? null : Expressions.name(field);
    }

    /** The field a single-field leaf ({@code MATCH}/{@code MATCH_PHRASE}) queries, or {@code null} for {@code QSTR}/{@code KQL}. */
    private static Expression leafField(Expression fullTextLeaf) {
        return switch (fullTextLeaf) {
            case Match match -> match.field();
            case MatchPhrase matchPhrase -> matchPhrase.field();
            case QueryString queryString -> null;
            case Kql kql -> null;
            default -> throw new IllegalStateException(
                "leafField: unexpected full-text leaf [" + fullTextLeaf.getClass().getSimpleName() + "]"
            );
        };
    }

    /**
     * The analyzer that highlights a leaf's field: its explicit {@code analyzer} option, or - for a
     * {@code MATCH}/{@code MATCH_PHRASE} leaf without one - the field's declared values analyzer. A {@code TO_TEXT}
     * column defaults its query analyzer to the values analyzer (see {@code SingleFieldFullTextFunction}), so the
     * borrowed match ran with that analyzer; highlighting must reproduce it or the snippet comes back empty.
     * {@code null} when the leaf names neither.
     */
    private static String effectiveFieldAnalyzerName(Expression fullTextLeaf) {
        String option = analyzerNameOf(fullTextLeaf);
        if (option != null) {
            return option;
        }
        Expression field = leafField(fullTextLeaf);
        return field == null ? null : AnalyzedTextExpression.valuesAnalyzerOf(field);
    }

    /**
     * Implicit query from an upstream WHERE, or {@code reasonIfMissing} when none was borrowed.
     *
     * @param query             the borrowed query (an {@code OR} of the qualifying conjuncts), or {@code null} when
     *                          none was found
     * @param reasonIfMissing   when {@code query} is {@code null}, a user-facing explanation of why nothing was
     *                          borrowed for HIGHLIGHT's post-analysis verification to report; {@code null} when a
     *                          query was found
     * @param analyzerName      shared non-default analyzer of the borrowed leaves, or {@code null}
     */
    public record ImplicitQuery(@Nullable Expression query, @Nullable String reasonIfMissing, @Nullable String analyzerName) {
        public ImplicitQuery {
            assert (query == null) == (reasonIfMissing != null);
            assert query != null || analyzerName == null;
        }
    }

    /**
     * Walks {@link DocPreserving} plans and ORs borrowable full-text conjuncts. Stops when rows no longer map to documents.
     * Rewrites conjuncts through intervening {@code RENAME}/{@code MV_EXPAND} and drops those whose field name was reused.
     */
    public static ImplicitQuery collectImplicitQuery(LogicalPlan child, Source source) {
        List<Expression> predicates = new ArrayList<>();
        boolean sawUnborrowableFullText = false;
        Set<String> redefinedFields = new LinkedHashSet<>();
        AttributeSet available = AttributeSet.of(child.output());
        Set<String> availableNames = available.names();
        AttributeMap.Builder<Attribute> lineage = AttributeMap.builder();
        LogicalPlan current = child;
        while (current instanceof DocPreserving docPreserving) {
            if (current instanceof Filter filter) {
                AttributeMap<Attribute> renames = lineage.build();
                for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
                    if (isSupportedImplicitPredicate(conjunct)) {
                        Expression rebound = renames.isEmpty()
                            ? conjunct
                            : conjunct.transformUp(Attribute.class, a -> renames.resolve(a, a));
                        if (namesRedefinedColumn(rebound, available, availableNames, redefinedFields) == false) {
                            predicates.add(rebound);
                        }
                    } else if (conjunct.anyMatch(e -> e instanceof FullTextFunction)) {
                        sawUnborrowableFullText = true;
                    }
                }
            } else {
                collectLineage(current, lineage);
            }
            current = docPreserving.preservingInput();
        }
        LogicalPlan blockedBy = current.children().isEmpty() ? null : current;

        if (predicates.isEmpty() == false) {
            Expression combined = Predicates.combineOrWithSource(predicates, source);
            return new ImplicitQuery(combined, null, uniformAnalyzerOf(combined));
        }
        return new ImplicitQuery(null, missingQueryReason(sawUnborrowableFullText, redefinedFields, blockedBy), null);
    }

    /** Maps a {@code RENAME} or {@code MV_EXPAND} column to the attribute that now holds its data. */
    private static void collectLineage(LogicalPlan node, AttributeMap.Builder<Attribute> lineage) {
        if (node instanceof Project project) {
            AttributeSet output = AttributeSet.of(project.output());
            for (NamedExpression projection : project.projections()) {
                if (projection instanceof Alias alias && alias.child() instanceof Attribute source && output.contains(source) == false) {
                    lineage.put(source, alias.toAttribute());
                }
            }
        } else if (node instanceof MvExpand mvExpand) {
            lineage.put(mvExpand.target().toAttribute(), mvExpand.expanded());
        }
    }

    /** True when {@code conjunct} names a column whose id was replaced by a different attribute of the same name. */
    private static boolean namesRedefinedColumn(
        Expression conjunct,
        AttributeSet available,
        Set<String> availableNames,
        Set<String> redefinedFields
    ) {
        boolean redefined = false;
        for (Attribute reference : conjunct.references()) {
            if (available.contains(reference) == false && availableNames.contains(reference.name())) {
                redefinedFields.add(reference.name());
                redefined = true;
            }
        }
        return redefined;
    }

    private static String missingQueryReason(
        boolean sawUnborrowableFullText,
        Set<String> redefinedFields,
        @Nullable LogicalPlan blockedBy
    ) {
        if (blockedBy != null) {
            return "HIGHLIGHT cannot borrow the WHERE before ["
                + blockedBy.sourceText()
                + "] because that command does not preserve documents; add an explicit query";
        }
        if (redefinedFields.isEmpty() == false) {
            return "HIGHLIGHT cannot borrow the WHERE condition on "
                + redefinedFields
                + " because "
                + (redefinedFields.size() == 1 ? "that field was" : "those fields were")
                + " redefined after the WHERE; add an explicit query and ON clause";
        }
        if (sawUnborrowableFullText) {
            return "HIGHLIGHT found no borrowable condition in the preceding WHERE: only positive MATCH, MATCH_PHRASE, "
                + "QSTR or KQL conditions joined by AND/OR can be borrowed; NOT and mixed conditions cannot";
        }
        return "HIGHLIGHT requires a query or a preceding full-text WHERE (MATCH, MATCH_PHRASE, QSTR or KQL)";
    }

    /**
     * Text/keyword columns of {@code childrenOutput} in output order ({@code ON *} / omitted ON).
     * Skips metadata and synthetic union-type conversions ({@code $$...}) so ON * does not mint {@code highlight_$$...}.
     */
    public static List<NamedExpression> allHighlightableFields(List<Attribute> childrenOutput) {
        return List.copyOf(highlightableFieldsByName(childrenOutput).values());
    }

    private static Map<String, NamedExpression> highlightableFieldsByName(List<Attribute> childrenOutput) {
        LinkedHashMap<String, NamedExpression> byName = new LinkedHashMap<>();
        for (Attribute attr : childrenOutput) {
            if (DataType.isString(attr.dataType()) && attr instanceof MetadataAttribute == false && attr.synthetic() == false) {
                // putLast (not put): generated-column order follows last-seen position, so a colliding name moves to the end.
                byName.putLast(attr.name(), attr);
            }
        }
        return byName;
    }

    /**
     * Fields an omitted ON list resolves to: names the query mentions, or every highlightable column when it cannot
     * be narrowed (literal, KQL, QSTR, negative). Missing or non-string names are dropped.
     */
    public static List<NamedExpression> deriveFields(Expression query, List<Attribute> childrenOutput) {
        Map<String, NamedExpression> highlightable = highlightableFieldsByName(childrenOutput);
        Set<String> names = new LinkedHashSet<>();
        if (collectFieldNames(query, names, FieldWalk.DERIVE) == false) {
            return List.copyOf(highlightable.values());
        }
        List<NamedExpression> result = new ArrayList<>(names.size());
        for (String name : names) {
            NamedExpression field = highlightable.get(name);
            if (field != null) {
                result.add(field);
            }
        }
        return result;
    }

    /**
     * First named field that is not a highlightable column of {@code childrenOutput}, or {@code null} when the query
     * cannot be narrowed. For explicit queries only; borrowed WHERE queries skip this and let {@link #deriveFields} drop names.
     */
    public static @Nullable String unhighlightableQueryField(Expression query, List<Attribute> childrenOutput) {
        Set<String> names = new LinkedHashSet<>();
        if (collectFieldNames(query, names, FieldWalk.DERIVE) == false) {
            return null;
        }
        Map<String, NamedExpression> highlightable = highlightableFieldsByName(childrenOutput);
        for (String name : names) {
            if (highlightable.containsKey(name) == false) {
                return name;
            }
        }
        return null;
    }

    /** Concrete field names the query narrows to, or {@code null} for a literal, QSTR, KQL, or negative clause. */
    static @Nullable Set<String> queryFieldNames(Expression query) {
        Set<String> names = new LinkedHashSet<>();
        if (collectFieldNames(query, names, FieldWalk.DERIVE) == false) {
            return null;
        }
        return names;
    }

    /** Message when a resolved query produced no highlightable fields. */
    public static String noHighlightableFieldsMessage(@Nullable Expression query) {
        Set<String> queryNames = query == null ? null : queryFieldNames(query);
        if (queryNames == null || queryNames.isEmpty()) {
            return "HIGHLIGHT found no text or keyword fields to highlight; add an explicit ON clause";
        }
        return "HIGHLIGHT found no text or keyword fields to highlight: the derived query names "
            + whichIsAre(queryNames)
            + " not a text or keyword column of the input (it may have been renamed or dropped); add an explicit query and ON clause";
    }

    /** Message when an implicit query names only fields that are not highlighted, or {@code null}. */
    public static @Nullable String implicitQueryFieldMismatchMessage(Expression query, List<NamedExpression> fields) {
        Set<String> queryNames = queryFieldNames(query);
        if (queryNames == null || queryNames.isEmpty()) {
            return null;
        }
        if (fields.stream().anyMatch(f -> queryNames.contains(f.name()))) {
            return null;
        }
        return "HIGHLIGHT derived its query from a preceding WHERE, but that query targets only "
            + whichIsAre(queryNames)
            + " not among the highlighted fields "
            + fields.stream().map(NamedExpression::name).toList()
            + "; add an explicit query and ON clause";
    }

    private static String whichIsAre(Set<String> names) {
        return names.stream().toList() + ", which " + (names.size() == 1 ? "is" : "are");
    }

    /**
     * ON fields the query still translates against after unused generated columns are pruned.
     * {@code null}: cannot be narrowed (keep every remaining ON field). Empty: colon-free literal (unused ON fields can go).
     * Unlike {@link #deriveFields}, negative subtrees count: {@code MATCH(a) AND NOT MATCH(b)} still needs {@code b}.
     */
    public static @Nullable Set<String> fieldsRequiredForTranslation(Expression query) {
        if (query == null) {
            return Set.of();
        }
        Set<String> names = new LinkedHashSet<>();
        if (collectFieldNames(query, names, FieldWalk.TRANSLATE) == false) {
            return null;
        }
        return names;
    }

    private enum FieldWalk {
        DERIVE,
        TRANSLATE
    }

    private static boolean collectFieldNames(Expression query, Set<String> names, FieldWalk mode) {
        return switch (query) {
            case Match match -> {
                names.add(Expressions.name(match.field()));
                yield true;
            }
            case MatchPhrase matchPhrase -> {
                names.add(Expressions.name(matchPhrase.field()));
                yield true;
            }
            case QueryString queryString -> false;
            case Kql kql -> false;
            case Literal literal -> mode == FieldWalk.TRANSLATE && literalMayNameField(literal) == false;
            case BinaryLogic binary -> collectFieldNames(binary.left(), names, mode) && collectFieldNames(binary.right(), names, mode);
            case Not not when mode == FieldWalk.TRANSLATE -> collectFieldNames(not.field(), names, mode);
            case Not not -> collectDerivationFieldNames(not);
            default -> false;
        };
    }

    private static boolean collectDerivationFieldNames(Not not) {
        return not.anyMatch(e -> e instanceof QueryString) == false
            && collectFieldNames(not.field(), new LinkedHashSet<>(), FieldWalk.DERIVE);
    }

    private static boolean literalMayNameField(Literal literal) {
        if (DataType.isString(literal.dataType()) == false) {
            return true;
        }
        String text = BytesRefs.toString(literal.value());
        return text != null && text.indexOf(':') >= 0;
    }

    /** Folded {@code default_field} of a QSTR, or {@code null}. May be a wildcard. */
    public static String queryStringDefaultField(QueryString queryString) {
        return foldedOption(queryString.options(), "default_field");
    }
}
