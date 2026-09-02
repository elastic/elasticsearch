/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.highlight;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.regex.Regex;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Analysis-time helpers for implicit HIGHLIGHT query and field lists. Does not use {@code SearchExecutionContext}.
 */
public final class HighlightSupport {

    private HighlightSupport() {}

    public static boolean isSupportedImplicitPredicate(Expression expr) {
        return switch (expr) {
            case Match match -> true;
            case MatchPhrase matchPhrase -> true;
            case QueryString queryString -> true;
            case Kql kql -> true;
            case BinaryLogic binary -> isSupportedImplicitPredicate(binary.left()) && isSupportedImplicitPredicate(binary.right());
            default -> false;
        };
    }

    /**
     * Every text or keyword column of {@code childrenOutput}, in output order. This is what {@code ON *} expands to,
     * and what an omitted ON list falls back to. Metadata attributes are excluded because they are not document
     * content, so highlighting them says nothing about why a row matched.
     */
    public static List<NamedExpression> allHighlightableFields(List<Attribute> childrenOutput) {
        LinkedHashMap<String, NamedExpression> byName = new LinkedHashMap<>();
        for (Attribute attr : childrenOutput) {
            if (DataType.isString(attr.dataType()) && attr instanceof MetadataAttribute == false) {
                // putLast (not put): generated-column order follows last-seen position, so a colliding name moves to the end.
                byName.putLast(attr.name(), attr);
            }
        }
        return List.copyOf(byName.values());
    }

    /**
     * The fields an omitted ON list resolves to: the ones the query names, or - when it names none - every
     * highlightable column. A query that cannot be narrowed to concrete fields (a string literal, {@code KQL}, a
     * {@code QSTR} without a concrete {@code default_field}, or anything else the walk does not recognise) may match
     * through any column, so falling back to all of them is closer to intent than highlighting nothing.
     * <p>
     * Negative subtrees contribute no names, and names the child output does not carry are dropped. Either can leave
     * the result empty, which HIGHLIGHT's post-analysis verification reports as a request for an explicit ON clause.
     */
    public static List<NamedExpression> deriveFields(Expression query, List<Attribute> childrenOutput) {
        List<NamedExpression> highlightable = allHighlightableFields(childrenOutput);
        Set<String> names = new LinkedHashSet<>();
        if (collectQueryFieldNames(query, names) == false) {
            return highlightable;
        }
        Map<String, NamedExpression> byName = new HashMap<>();
        for (NamedExpression field : highlightable) {
            byName.put(field.name(), field);
        }
        List<NamedExpression> result = new ArrayList<>(names.size());
        for (String name : names) {
            NamedExpression field = byName.get(name);
            if (field != null) {
                result.add(field);
            }
        }
        return result;
    }

    private static boolean collectQueryFieldNames(Expression query, Set<String> names) {
        return switch (query) {
            case Match match -> {
                names.add(Expressions.name(match.field()));
                yield true;
            }
            case MatchPhrase matchPhrase -> {
                names.add(Expressions.name(matchPhrase.field()));
                yield true;
            }
            case QueryString queryString -> {
                String defaultField = queryStringDefaultField(queryString);
                if (defaultField == null || Regex.isSimpleMatchPattern(defaultField)) {
                    yield false;
                }
                names.add(defaultField);
                yield true;
            }
            case Kql kql -> false;
            case Literal literal -> false;
            case BinaryLogic binary -> collectQueryFieldNames(binary.left(), names) && collectQueryFieldNames(binary.right(), names);
            // A negative clause says which docs to exclude, not which fields to highlight: contribute no names, but
            // yield true so it does not force the all-fields fallback the way an unrecognised expression does.
            case Not not -> true;
            default -> false;
        };
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
