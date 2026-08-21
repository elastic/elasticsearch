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
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Pure logical-plan/expression utilities for resolving HIGHLIGHT's implicit query and derived fields during
 * analysis. Kept separate from {@code HighlightQueryBuilders} (the planner package), which owns everything that
 * depends on a {@code SearchExecutionContext}: translation to Query DSL and structural/on-field verification. Only
 * that dependency is a genuine planning concern; the walks here operate on logical-plan output and expressions.
 */
public final class HighlightSupport {

    private HighlightSupport() {}

    /** Returns whether {@code expr} can contribute to an implicit HIGHLIGHT query. */
    public static boolean isSupportedImplicitPredicate(Expression expr) {
        return switch (expr) {
            case Match match -> true;
            case MatchPhrase matchPhrase -> true;
            case QueryString queryString -> true;
            case Kql kql -> true;
            case And and -> isSupportedImplicitPredicate(and.left()) && isSupportedImplicitPredicate(and.right());
            case Or or -> isSupportedImplicitPredicate(or.left()) && isSupportedImplicitPredicate(or.right());
            default -> false;
        };
    }

    /** Returns all non-metadata string fields, using the last field when names collide. */
    public static List<NamedExpression> allHighlightableFields(List<Attribute> childrenOutput) {
        Map<String, NamedExpression> byName = new LinkedHashMap<>();
        for (Attribute attr : childrenOutput) {
            if (DataType.isString(attr.dataType()) && attr instanceof MetadataAttribute == false) {
                // remove+put is deliberate, not just last-wins dedup: it also moves the collision to the end of the
                // LinkedHashMap's iteration order, which is generated-column order. A plain put would overwrite in
                // place and keep the first-seen position instead.
                byName.remove(attr.name());
                byName.put(attr.name(), attr);
            }
        }
        return List.copyOf(byName.values());
    }

    /**
     * Returns the query's string fields, or all string fields when the query does not name a concrete field. Field
     * selection for a named field is defined in terms of {@link #allHighlightableFields}, so a query naming a
     * duplicated column resolves to the same attribute the {@code ON *} form would pick (last-wins), and a query
     * naming a metadata field (e.g. {@code _index}) derives nothing for it, consistent with {@code ON *} excluding
     * metadata columns.
     */
    public static List<NamedExpression> deriveFields(Expression query, List<Attribute> childrenOutput) {
        Set<String> names = new LinkedHashSet<>();
        boolean fieldsKnown = collectQueryFieldNames(query, names);
        if (fieldsKnown == false) {
            return allHighlightableFields(childrenOutput);
        }
        Map<String, NamedExpression> highlightable = allHighlightableFields(childrenOutput).stream()
            .collect(Collectors.toMap(NamedExpression::name, ne -> ne, (first, last) -> last, LinkedHashMap::new));
        List<NamedExpression> result = new ArrayList<>(names.size());
        for (String name : names) {
            NamedExpression attr = highlightable.get(name);
            if (attr != null) {
                result.add(attr);
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
            case And and -> {
                boolean left = collectQueryFieldNames(and.left(), names);
                boolean right = collectQueryFieldNames(and.right(), names);
                yield left && right;
            }
            case Or or -> {
                boolean left = collectQueryFieldNames(or.left(), names);
                boolean right = collectQueryFieldNames(or.right(), names);
                yield left && right;
            }
            case Not not -> true;
            default -> false;
        };
    }

    /** The query's {@code default_field} option when it folds to a concrete string, or {@code null} otherwise. */
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
