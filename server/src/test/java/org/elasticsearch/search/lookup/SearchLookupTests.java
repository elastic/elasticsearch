/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class SearchLookupTests extends ESTestCase {
    private static SearchLookup lookup(MappedFieldType fieldType) {
        return new SearchLookup(
            name -> fieldType,
            name -> true,
            (type, searchLookup, operation) -> null,
            null,
            LeafFieldLookupProvider.fromStoredFields()
        );
    }

    public void testWithoutOnlyMappedAsRuntimeFieldReportsNoFieldAsOnlyRuntime() {
        SearchLookup lookup = lookup(null);
        assertThat(lookup.onlyMappedAsRuntimeField("a"), is(true));
        assertThat(lookup.withoutOnlyMappedAsRuntimeField().onlyMappedAsRuntimeField("a"), is(false));
        assertThat(lookup.onlyMappedAsRuntimeField("a"), is(true));
    }

    public void testWithoutOnlyMappedAsRuntimeFieldLooksFieldsUpTheSameWay() {
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("a");
        SearchLookup copy = lookup(fieldType).withoutOnlyMappedAsRuntimeField();
        assertThat(copy.fieldType("a"), sameInstance(fieldType));
    }

    public void testWithoutOnlyMappedAsRuntimeFieldKeepsTheTrackedFieldReferences() {
        SearchLookup copy = lookup(null).forkAndTrackFieldReferences("a").withoutOnlyMappedAsRuntimeField();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> copy.forkAndTrackFieldReferences("a"));
        assertThat(e.getMessage(), containsString("Cyclic dependency detected"));
    }
}
