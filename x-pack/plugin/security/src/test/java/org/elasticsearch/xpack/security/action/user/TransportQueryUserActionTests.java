/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class TransportQueryUserActionTests extends ESTestCase {
    private static final String[] allowedIndexFieldNames = new String[] {
        "username",
        "roles",
        "full_name",
        "email",
        "enabled" };

    public void testTranslateFieldSortBuilders() {
        final List<String> fieldNames = List.of(allowedIndexFieldNames);

        final List<FieldSortBuilder> originals = fieldNames.stream().map(this::randomFieldSortBuilderWithName).toList();

        final SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        TransportQueryUserAction.translateFieldSortBuilders(originals, searchSourceBuilder);

        IntStream.range(0, originals.size()).forEach(i -> {
            final FieldSortBuilder original = originals.get(i);
            final FieldSortBuilder translated = (FieldSortBuilder) searchSourceBuilder.sorts().get(i);
            assertThat(original.getFieldName(), equalTo(translated.getFieldName()));

            assertThat(translated.order(), equalTo(original.order()));
            assertThat(translated.missing(), equalTo(original.missing()));
            assertThat(translated.unmappedType(), equalTo(original.unmappedType()));
            assertThat(translated.getNumericType(), equalTo(original.getNumericType()));
            assertThat(translated.getFormat(), equalTo(original.getFormat()));
            assertThat(translated.sortMode(), equalTo(original.sortMode()));
        });
    }

    public void testNestedSortingIsNotAllowed() {
        final FieldSortBuilder fieldSortBuilder = new FieldSortBuilder("roles");
        fieldSortBuilder.setNestedSort(new NestedSortBuilder("something"));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportQueryUserAction.translateFieldSortBuilders(List.of(fieldSortBuilder), SearchSourceBuilder.searchSource())
        );
        assertThat(e.getMessage(), equalTo("nested sorting is not supported for User query"));
    }

    private FieldSortBuilder randomFieldSortBuilderWithName(String name) {
        final FieldSortBuilder fieldSortBuilder = new FieldSortBuilder(name);
        fieldSortBuilder.order(randomBoolean() ? SortOrder.ASC : SortOrder.DESC);
        fieldSortBuilder.setFormat(randomBoolean() ? randomAlphaOfLengthBetween(3, 16) : null);
        if (randomBoolean()) {
            fieldSortBuilder.setNumericType(randomFrom("long", "double", "date", "date_nanos"));
        }
        if (randomBoolean()) {
            fieldSortBuilder.missing(randomAlphaOfLengthBetween(3, 8));
        }
        if (randomBoolean()) {
            fieldSortBuilder.sortMode(randomFrom(SortMode.values()));
        }
        return fieldSortBuilder;
    }
}
