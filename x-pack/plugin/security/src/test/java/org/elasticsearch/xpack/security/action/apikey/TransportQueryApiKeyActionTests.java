/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.security.support.FieldNameTranslators.API_KEY_FIELD_NAME_TRANSLATORS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class TransportQueryApiKeyActionTests extends ESTestCase {

    public void testTranslateFieldSortBuilders() {
        final String metadataField = randomAlphaOfLengthBetween(3, 8);
        final List<String> fieldNames = List.of(
            "_doc",
            "username",
            "realm_name",
            "name",
            "creation",
            "expiration",
            "type",
            "invalidated",
            "metadata." + metadataField
        );

        final List<FieldSortBuilder> originals = fieldNames.stream().map(this::randomFieldSortBuilderWithName).toList();

        List<String> sortFields = new ArrayList<>();
        final SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        API_KEY_FIELD_NAME_TRANSLATORS.translateFieldSortBuilders(originals, searchSourceBuilder, sortFields::add);

        IntStream.range(0, originals.size()).forEach(i -> {
            final FieldSortBuilder original = originals.get(i);
            final FieldSortBuilder translated = (FieldSortBuilder) searchSourceBuilder.sorts().get(i);
            if (Set.of("_doc", "name").contains(original.getFieldName())) {
                assertThat(translated, equalTo(original));
            } else {
                if ("username".equals(original.getFieldName())) {
                    assertThat(translated.getFieldName(), equalTo("creator.principal"));
                } else if ("realm_name".equals(original.getFieldName())) {
                    assertThat(translated.getFieldName(), equalTo("creator.realm"));
                } else if ("creation".equals(original.getFieldName())) {
                    assertThat(translated.getFieldName(), equalTo("creation_time"));
                } else if ("expiration".equals(original.getFieldName())) {
                    assertThat(translated.getFieldName(), equalTo("expiration_time"));
                } else if ("invalidated".equals(original.getFieldName())) {
                    assertThat(translated.getFieldName(), equalTo("api_key_invalidated"));
                } else if (original.getFieldName().startsWith("metadata.")) {
                    assertThat(translated.getFieldName(), equalTo("metadata_flattened." + original.getFieldName().substring(9)));
                } else if ("type".equals(original.getFieldName())) {
                    assertThat(translated.getFieldName(), equalTo("runtime_key_type"));
                } else {
                    fail("unrecognized field name: [" + original.getFieldName() + "]");
                }
                assertThat(translated.order(), equalTo(original.order()));
                assertThat(translated.missing(), equalTo(original.missing()));
                assertThat(translated.unmappedType(), equalTo(original.unmappedType()));
                assertThat(translated.getNumericType(), equalTo(original.getNumericType()));
                assertThat(translated.getFormat(), equalTo(original.getFormat()));
                assertThat(translated.sortMode(), equalTo(original.sortMode()));
            }
        });
        assertThat(
            sortFields,
            containsInAnyOrder(
                "creator.principal",
                "creator.realm",
                "name",
                "creation_time",
                "expiration_time",
                "runtime_key_type",
                "api_key_invalidated",
                "metadata_flattened." + metadataField
            )
        );
    }

    public void testNestedSortingIsNotAllowed() {
        final FieldSortBuilder fieldSortBuilder = new FieldSortBuilder("name");
        fieldSortBuilder.setNestedSort(new NestedSortBuilder("name"));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> API_KEY_FIELD_NAME_TRANSLATORS.translateFieldSortBuilders(
                List.of(fieldSortBuilder),
                SearchSourceBuilder.searchSource(),
                ignored -> {}
            )
        );
        assertThat(e.getMessage(), equalTo("nested sorting is not currently supported in this context"));
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
