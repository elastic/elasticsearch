/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.highlight;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
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
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.plan.logical.DocPreserving;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.query.MatchQueryBuilder.ANALYZER_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.QUOTE_ANALYZER_FIELD;

/**
 * Analysis-time helpers for implicit HIGHLIGHT query and field lists. Does not use {@code SearchExecutionContext}.
 */
public final class HighlightSupport {

    private HighlightSupport() {}

    /**
     * Whether a {@code WHERE} conjunct can be borrowed as (part of) an implicit HIGHLIGHT query. Only positive
     * full-text predicates qualify, because the highlight query is display, not selection: a {@code NOT} says which
     * rows to exclude and contributes no spans, and a non-full-text leaf (comparison, literal) has nothing to
     * highlight. {@code AND}/{@code OR} of qualifying predicates qualify as a whole; if either side does not, the whole
     * conjunct is skipped.
     * <p>
     * A predicate carrying an {@code analyzer} or {@code quote_analyzer} option is not borrowed: HIGHLIGHT translates
     * against a synthetic context that knows only its own analyzer, so those options cannot be applied correctly today.
     * {@link #collectImplicitQuery} reports that as an error rather than silently dropping the conjunct.
     * TODO: support WHERE-side analyzer/quote_analyzer on an implicit HIGHLIGHT query (copy onto HIGHLIGHT when the
     * name is unique; per-field MemoryIndex analysis when names differ).
     * <p>
     * This is deliberately narrower than what an explicit HIGHLIGHT query accepts (which allows {@code NOT} and mixed
     * boolean combinations, see {@link org.elasticsearch.xpack.esql.planner.HighlightQueryBuilders#build}): the implicit
     * query is a best-effort convenience derived from selection predicates, so it only borrows shapes whose intent to
     * highlight is unambiguous.
     * <p>
     * Must stay in sync with {@link org.elasticsearch.xpack.esql.planner.HighlightQueryBuilders#build}: everything
     * accepted here must be translatable there.
     */
    public static boolean isSupportedImplicitPredicate(Expression expr) {
        if (expr instanceof BinaryLogic binary) {
            return isSupportedImplicitPredicate(binary.left()) && isSupportedImplicitPredicate(binary.right());
        }
        return isBorrowableFullText(expr) && hasAnalyzerOption(expr) == false;
    }

    private static boolean isBorrowableFullText(Expression expr) {
        return expr instanceof Match || expr instanceof MatchPhrase || expr instanceof QueryString || expr instanceof Kql;
    }

    /**
     * Whether a full-text leaf carries an {@code analyzer} or {@code quote_analyzer} option.
     */
    private static boolean hasAnalyzerOption(Expression expr) {
        Expression options = switch (expr) {
            case Match match -> match.options();
            case MatchPhrase matchPhrase -> matchPhrase.options();
            case QueryString queryString -> queryString.options();
            case Kql kql -> kql.options();
            default -> null;
        };
        return options instanceof MapExpression map
            && (map.get(ANALYZER_FIELD.getPreferredName()) != null || map.get(QUOTE_ANALYZER_FIELD.getPreferredName()) != null);
    }

    /**
     * Result of walking the doc-preserving chain above HIGHLIGHT to derive an implicit query from an upstream WHERE.
     *
     * @param query           the borrowed query (an {@code OR} of the qualifying conjuncts), or {@code null} when none
     *                        was found
     * @param reasonIfMissing when {@code query} is {@code null}, a user-facing explanation of why nothing was borrowed
     *                        for HIGHLIGHT's post-analysis verification to report; {@code null} when a query was found
     */
    public record ImplicitQuery(@Nullable Expression query, @Nullable String reasonIfMissing) {
        public ImplicitQuery {
            assert (query == null) == (reasonIfMissing != null);
        }
    }

    /**
     * Collects the searchable conjuncts of every {@code WHERE} that still describes the documents reaching HIGHLIGHT.
     * The walk moves down the child chain (children are upstream) and stops at the first node that is not
     * {@link DocPreserving}, because past that point a row no longer maps to a single document. Non-unary barriers
     * such as {@code LOOKUP JOIN} and {@code FORK} also end the walk and are recorded as the blocker so the
     * missing-query message can name them, matching unary barriers such as {@code STATS}.
     * <p>
     * Conjuncts are collected as-is, with no check that their fields are still live: a predicate whose field was later
     * dropped or renamed translates against a context that only knows the ON fields, so it becomes a match-none query
     * and the column comes out null. An {@code AttributeSet} liveness guard would be worse, since membership is
     * {@code NameId}-based and RENAME or MV_EXPAND mint fresh ids, silently dropping predicates.
     * <p>
     * WHEREs filter conjunctively, but highlight terms are OR-ed because the highlight query is display, not selection.
     * The combined query is given {@code source} (HIGHLIGHT's own location) so a translation failure points at the
     * command rather than at an arbitrary borrowed conjunct inside a WHERE.
     * <p>
     * A WHERE-side {@code analyzer} or {@code quote_analyzer} option cannot be borrowed today; the walk returns an
     * error rather than dropping that conjunct or adopting the name onto HIGHLIGHT.
     *
     * @param source HIGHLIGHT's source, used as the location of the combined query
     */
    public static ImplicitQuery collectImplicitQuery(LogicalPlan child, Source source) {
        List<Expression> predicates = new ArrayList<>();
        boolean sawUnborrowableFullText = false;
        boolean sawAnalyzerOption = false;
        LogicalPlan blockedBy = null;
        LogicalPlan current = child;
        while (current instanceof UnaryPlan) {
            if (current instanceof DocPreserving docPreserving) {
                if (current instanceof Filter filter) {
                    if (filter.condition().anyMatch(HighlightSupport::hasAnalyzerOption)) {
                        sawAnalyzerOption = true;
                    }
                    if (borrowConjuncts(filter, predicates) == false && filter.condition().anyMatch(e -> e instanceof FullTextFunction)) {
                        sawUnborrowableFullText = true;
                    }
                }
                current = docPreserving.preservingInput();
            } else {
                blockedBy = current;
                break;
            }
        }
        // Unary barriers are recorded in the loop. LOOKUP JOIN / FORK are not UnaryPlan, so the loop just ends
        // on them; a source relation is a leaf and is not a blocker.
        if (blockedBy == null && current.children().isEmpty() == false) {
            blockedBy = current;
        }
        // Do not partially borrow: a sibling MATCH without an analyzer would otherwise become the implicit query
        // and silently drop the analyzer-bearing conjunct.
        if (sawAnalyzerOption) {
            return new ImplicitQuery(
                null,
                "HIGHLIGHT cannot borrow a WHERE condition that sets analyzer or quote_analyzer; add an explicit HIGHLIGHT query"
            );
        }
        if (predicates.isEmpty() == false) {
            return new ImplicitQuery(Predicates.combineOrWithSource(predicates, source), null);
        }
        return new ImplicitQuery(null, missingQueryReason(sawUnborrowableFullText, blockedBy));
    }

    private static boolean borrowConjuncts(Filter filter, List<Expression> predicates) {
        boolean borrowed = false;
        for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
            if (isSupportedImplicitPredicate(conjunct)) {
                predicates.add(conjunct);
                borrowed = true;
            }
        }
        return borrowed;
    }

    private static String missingQueryReason(boolean sawUnborrowableFullText, @Nullable LogicalPlan blockedBy) {
        if (blockedBy != null) {
            return "HIGHLIGHT cannot borrow the WHERE before ["
                + blockedBy.sourceText()
                + "] because that command does not preserve documents; add an explicit query";
        }
        if (sawUnborrowableFullText) {
            return "HIGHLIGHT found no borrowable condition in the preceding WHERE: only positive MATCH, MATCH_PHRASE, "
                + "QSTR or KQL conditions joined by AND/OR can be borrowed; NOT and mixed conditions cannot";
        }
        return "HIGHLIGHT requires a query or a preceding full-text WHERE (MATCH, MATCH_PHRASE, QSTR or KQL)";
    }

    /**
     * Every text or keyword column of {@code childrenOutput}, in output order. This is what {@code ON *} expands to,
     * and what an omitted ON list falls back to. Metadata attributes are excluded because they are not document
     * content, so highlighting them says nothing about why a row matched. Synthetic attributes are excluded too: a
     * union-type conversion appends attributes such as {@code $$title$converted_to$keyword} to the relation output,
     * and expanding over them would mint a {@code highlight_$$title$converted_to$keyword} column targeting a field
     * that does not exist.
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
     * The fields an omitted ON list resolves to: the ones the query names, or - when it names none - every
     * highlightable column. A query that cannot be narrowed to concrete fields (a string literal, {@code KQL}, any
     * {@code QSTR}, a negative clause, or anything else the walk does not recognise) may match through any column, so
     * falling back to all of them is closer to intent than highlighting nothing.
     * <p>
     * Negative subtrees contribute no names, and names the child output does not carry are dropped. Either can leave
     * the result empty, which HIGHLIGHT's post-analysis verification reports as a request for an explicit ON clause.
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
     * The first field a resolvable query names that is not a highlightable column of {@code childrenOutput} - either
     * because the type is not text/keyword, or (in principle) because the column is absent, though an absent column
     * fails query resolution before this is reached. {@code null} when the query cannot be narrowed to concrete fields (a
     * literal, {@code KQL}, a {@code QSTR}, or a negative clause), since those fall back to every highlightable column
     * and name nothing specific to reject. Callers surface the result through the unresolved-attribute channel so
     * verification points at the offending field rather than reporting a generic "no fields to highlight".
     * <p>
     * This is for queries the user wrote on the command. A query derived from an upstream {@code WHERE} may name
     * non-text fields legitimately, so callers skip this check there and let {@link #deriveFields} drop those names.
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

    /**
     * The concrete field names a resolvable query narrows to, or {@code null} when it cannot be narrowed (a string
     * literal, {@code QSTR}, {@code KQL}, or a negative clause). Names preserve query order.
     */
    static @Nullable Set<String> queryFieldNames(Expression query) {
        Set<String> names = new LinkedHashSet<>();
        if (collectFieldNames(query, names, FieldWalk.DERIVE) == false) {
            return null;
        }
        return names;
    }

    /**
     * Message when a resolved query produced no highlightable fields. Names the query's fields when they can be
     * narrowed, instead of only advising an ON clause that would yield an all-null column.
     */
    public static String noHighlightableFieldsMessage(@Nullable Expression query) {
        Set<String> queryNames = query == null ? null : queryFieldNames(query);
        if (queryNames == null || queryNames.isEmpty()) {
            return "HIGHLIGHT found no text or keyword fields to highlight; add an explicit ON clause";
        }
        return "HIGHLIGHT found no text or keyword fields to highlight: the derived query names "
            + whichIsAre(queryNames)
            + " not a text or keyword column of the input (it may have been renamed or dropped); add an explicit query and ON clause";
    }

    /**
     * Message when an implicit query narrows to concrete fields none of which is highlighted, or {@code null} when
     * the query also names a highlighted field or cannot be narrowed ({@code QSTR}, {@code KQL}, a literal).
     */
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
     * ON field names the query still has to translate against after unused generated columns are pruned.
     * {@code null} means the query cannot be narrowed ({@code QSTR}, {@code KQL}, a {@code field:term} string
     * literal, or an unrecognised shape) and every remaining ON field must stay in the translation context. An
     * empty set means a literal that names no field: it is applied to whatever ON fields survive, so unused ones
     * can go.
     * <p>
     * Unlike {@link #deriveFields}, negative subtrees count. {@code MATCH(a) AND NOT MATCH(b)} still
     * translates {@code MATCH(b)}, so {@code b} cannot be dropped from ON even when {@code highlight_b}
     * is unused.
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

    /**
     * The {@code default_field} option of a {@code QSTR}, or {@code null} when it is absent or does not fold to a
     * constant. The value may be a wildcard pattern; callers decide whether that still identifies a single field.
     */
    public static String queryStringDefaultField(QueryString queryString) {
        if (queryString.options() instanceof MapExpression map) {
            Expression value = map.get("default_field");
            if (value != null && value.foldable()) {
                return BytesRefs.toString(value.fold(FoldContext.small()));
            }
        }
        return null;
    }
}
