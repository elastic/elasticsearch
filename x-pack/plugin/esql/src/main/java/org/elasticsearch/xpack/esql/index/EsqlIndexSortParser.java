/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.apache.lucene.search.SortField;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexSortConfig.SortEntry;
import org.elasticsearch.index.IndexSortParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * Parses the {@link IndexSortConfig#INDEX_SORT_SETTING} expression as an ES|QL {@code SORT}. A key is a field when ES|QL reads
 * it as a field, or as {@code MV_MIN} or {@code MV_MAX} of one, and any other expression is an {@link EsqlSortKey}.
 */
public final class EsqlIndexSortParser implements IndexSortParser {

    @Override
    public List<SortEntry> parse(String expression) {
        List<SortEntry> entries = new ArrayList<>();
        // The kind of key and its field or text, so a quoted field named MV_MAX(a) isn't MV_MAX of field a
        Set<List<String>> keys = new HashSet<>();
        for (Order order : EsqlSortKey.parseSort(expression)) {
            SortOrder sortOrder = order.direction() == Order.OrderDirection.DESC ? SortOrder.DESC : SortOrder.ASC;
            String missing = order.nullsPosition() == Order.NullsPosition.FIRST ? "_first" : "_last";
            Expression key = order.child();
            String text = key.source().text();
            List<String> identity;
            SortEntry entry;
            if (key instanceof UnresolvedAttribute field) {
                identity = List.of("", field.name());
                MultiValueMode mode = sortOrder == SortOrder.DESC ? MultiValueMode.MAX : MultiValueMode.MIN;
                entry = SortEntry.onField(field.name(), sortOrder, mode, missing);
            } else if (multiValueMode(key) instanceof MultiValueMode mode) {
                String field = ((UnresolvedAttribute) key.children().getFirst()).name();
                identity = List.of(mode.name(), field);
                entry = SortEntry.onField(field, sortOrder, mode, missing);
            } else {
                identity = List.of("computed", text);
                entry = SortEntry.onComputedKey(text, sortOrder, missing, EsqlSortKey.validate(text, key));
            }
            if (keys.add(identity) == false) {
                throw new IllegalArgumentException(
                    "setting [" + IndexSortConfig.INDEX_SORT_SETTING.getKey() + "] contains sort key [" + text + "] more than once"
                );
            }
            entries.add(entry);
        }
        return entries;
    }

    /** The mode of {@code MV_MIN} or {@code MV_MAX} of a field, or {@code null} for any other key. */
    private static MultiValueMode multiValueMode(Expression key) {
        if (key instanceof UnresolvedFunction function
            && function.children().size() == 1
            && function.children().getFirst() instanceof UnresolvedAttribute) {
            return switch (function.name().toUpperCase(Locale.ROOT)) {
                case "MV_MIN" -> MultiValueMode.MIN;
                case "MV_MAX" -> MultiValueMode.MAX;
                default -> null;
            };
        }
        return null;
    }

    @Override
    public SortField resolve(
        String computedKey,
        boolean reverse,
        boolean nullsFirst,
        IndexSettings indexSettings,
        Function<String, MappedFieldType> fieldTypeLookup
    ) {
        return new EsqlSortField(EsqlSortKey.resolve(computedKey, indexSettings, fieldTypeLookup), reverse, nullsFirst);
    }
}
