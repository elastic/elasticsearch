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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Analysis-time helpers for derived HIGHLIGHT field lists. Does not use {@code SearchExecutionContext}.
 */
public final class HighlightSupport {

    private HighlightSupport() {}

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
