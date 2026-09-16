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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.query.MatchQueryBuilder.ANALYZER_FIELD;

/** Analysis-time helpers for implicit HIGHLIGHT query and field lists. */
public final class HighlightSupport {

    private HighlightSupport() {}

    /** Positive MATCH, MATCH_PHRASE, QSTR, KQL, and AND/OR of those. Not NOT or mixed predicates. */
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
            case Kql kql -> kql.options();
            default -> null;
        };
        return foldedOption(options, ANALYZER_FIELD.getPreferredName());
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
     * Analyzer every named full-text leaf agrees on, or {@code null} if none name one or they disagree.
     * Unlabeled leaves do not constrain the result. Disagreement is reported by {@link #requireUniformAnalyzer}.
     */
    public static @Nullable String uniformAnalyzerOf(Expression query) {
        Set<String> named = namedLeafAnalyzers(query);
        return named.size() == 1 ? named.iterator().next() : null;
    }

    /**
     * Named leaf analyzers must equal {@code commandAnalyzerName} when set, or all share one name when it is not.
     *
     * @throws IllegalArgumentException when they disagree
     */
    public static void requireUniformAnalyzer(Expression query, @Nullable String commandAnalyzerName) {
        Set<String> named = namedLeafAnalyzers(query);
        if (commandAnalyzerName != null) {
            for (String leaf : named) {
                if (leaf.equals(commandAnalyzerName) == false) {
                    throw new IllegalArgumentException(
                        "HIGHLIGHT WITH analyzer ["
                            + commandAnalyzerName
                            + "] does not match analyzer ["
                            + leaf
                            + "] specified by the query; they must be the same"
                    );
                }
            }
            return;
        }
        if (named.size() > 1) {
            // Do not suggest WITH { "analyzer": ... } here: a single WITH value can never equal two distinct leaf analyzers, so
            // that advice contradicts the WITH branch above. Point at the only remedy that works instead.
            throw new IllegalArgumentException(
                "HIGHLIGHT full-text functions use different analyzers "
                    + named
                    + "; use the same analyzer for every clause, or write an explicit HIGHLIGHT query using a single analyzer"
            );
        }
    }

    private static Set<String> namedLeafAnalyzers(Expression query) {
        Set<String> names = new LinkedHashSet<>();
        query.forEachDown(FullTextFunction.class, leaf -> {
            String analyzer = analyzerNameOf(leaf);
            if (analyzer != null) {
                names.add(analyzer);
            }
        });
        return names;
    }

    /**
     * Implicit query from an upstream WHERE, or {@code reasonIfMissing} when none was borrowed.
     *
     * @param query           OR of borrowable conjuncts, or {@code null}
     * @param reasonIfMissing user-facing explanation when {@code query} is {@code null}
     */
    public record ImplicitQuery(@Nullable Expression query, @Nullable String reasonIfMissing) {
        public ImplicitQuery {
            assert (query == null) == (reasonIfMissing != null);
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
            return new ImplicitQuery(Predicates.combineOrWithSource(predicates, source), null);
        }
        return new ImplicitQuery(null, missingQueryReason(sawUnborrowableFullText, redefinedFields, blockedBy));
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
