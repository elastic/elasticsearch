/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.querydsl.query.MatchAll;
import org.elasticsearch.xpack.ql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.tree.SourceTests;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.hasEntry;

public class NestedQueryTests extends ESTestCase {
    static Query randomQuery(int depth) {
        List<Supplier<Query>> options = new ArrayList<>();
        options.add(MatchQueryTests::randomMatchQuery);
        if (depth > 0) {
            options.add(() -> randomNestedQuery(depth - 1));
            options.add(() -> BoolQueryTests.randomBoolQuery(depth - 1));
        }
        return randomFrom(options).get();
    }

    static NestedQuery randomNestedQuery(int depth) {
        return new NestedQuery(SourceTests.randomSource(), randomAlphaOfLength(5), randomFields(), randomQuery(depth));
    }

    private static Map<String, Map.Entry<Boolean, String>> randomFields() {
        int size = between(0, 5);
        Map<String, Map.Entry<Boolean, String>> fields = new HashMap<>(size);
        while (fields.size() < size) {
            fields.put(randomAlphaOfLength(5), new SimpleImmutableEntry<>(randomBoolean(), null));
        }
        return fields;
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(randomNestedQuery(5), NestedQueryTests::copy, NestedQueryTests::mutate);
    }

    private static NestedQuery copy(NestedQuery query) {
        return new NestedQuery(query.source(), query.path(), query.fields(), query.child());
    }

    private static NestedQuery mutate(NestedQuery query) {
        List<Function<NestedQuery, NestedQuery>> options = Arrays.asList(
            q -> new NestedQuery(SourceTests.mutate(q.source()), q.path(), q.fields(), q.child()),
            q -> new NestedQuery(q.source(), randomValueOtherThan(q.path(), () -> randomAlphaOfLength(5)), q.fields(), q.child()),
            q -> new NestedQuery(q.source(), q.path(), randomValueOtherThan(q.fields(), NestedQueryTests::randomFields), q.child()),
            q -> new NestedQuery(q.source(), q.path(), q.fields(), randomValueOtherThan(q.child(), () -> randomQuery(5))));
        return randomFrom(options).apply(query);
    }

    public void testContainsNestedField() {
        NestedQuery q = randomNestedQuery(0);
        for (String field : q.fields().keySet()) {
            assertTrue(q.containsNestedField(q.path(), field));
            assertFalse(q.containsNestedField(randomValueOtherThan(q.path(), () -> randomAlphaOfLength(5)), field));
        }
        assertFalse(q.containsNestedField(q.path(), randomValueOtherThanMany(q.fields()::containsKey, () -> randomAlphaOfLength(5))));
    }

    public void testAddNestedField() {
        NestedQuery q = randomNestedQuery(0);
        for (String field : q.fields().keySet()) {
            // add does nothing if the field is already there
            assertSame(q, q.addNestedField(q.path(), field, null, randomBoolean()));
            String otherPath = randomValueOtherThan(q.path(), () -> randomAlphaOfLength(5));
            // add does nothing if the path doesn't match
            assertSame(q, q.addNestedField(otherPath, randomAlphaOfLength(5), null, randomBoolean()));
        }

        // if the field isn't in the list then add rewrites to a query with all the old fields and the new one
        String newField = randomValueOtherThanMany(q.fields()::containsKey, () -> randomAlphaOfLength(5));
        boolean hasDocValues = randomBoolean();
        NestedQuery added = (NestedQuery) q.addNestedField(q.path(), newField, null, hasDocValues);
        assertNotSame(q, added);
        assertThat(added.fields(), hasEntry(newField, new SimpleImmutableEntry<>(hasDocValues, null)));
        assertTrue(added.containsNestedField(q.path(), newField));
        for (Map.Entry<String, Map.Entry<Boolean, String>> field : q.fields().entrySet()) {
            assertThat(added.fields(), hasEntry(field.getKey(), field.getValue()));
            assertTrue(added.containsNestedField(q.path(), field.getKey()));
        }
    }

    public void testEnrichNestedSort() {
        NestedQuery q = randomNestedQuery(0);

        // enrich adds the filter if the path matches
        {
            NestedSortBuilder sort = new NestedSortBuilder(q.path());
            q.enrichNestedSort(sort);
            assertEquals(q.child().asBuilder(), sort.getFilter());
        }

        // but doesn't if it doesn't match
        {
            NestedSortBuilder sort = new NestedSortBuilder(randomValueOtherThan(q.path(), () -> randomAlphaOfLength(5)));
            q.enrichNestedSort(sort);
            assertNull(sort.getFilter());
        }

        // enriching with the same query twice is fine
        {
            NestedSortBuilder sort = new NestedSortBuilder(q.path());
            q.enrichNestedSort(sort);
            assertEquals(q.child().asBuilder(), sort.getFilter());
            q.enrichNestedSort(sort);

            // But enriching using another query will keep only the first query
            Query originalChildQuery = randomValueOtherThan(q.child(), () -> randomQuery(0));
            NestedQuery other = new NestedQuery(SourceTests.randomSource(), q.path(), q.fields(), originalChildQuery);
            other.enrichNestedSort(sort);
            assertEquals(other.child().asBuilder(), originalChildQuery.asBuilder());
        }
    }

    public void testToString() {
        NestedQuery q = new NestedQuery(new Source(1, 1, StringUtils.EMPTY), "a.b",
                singletonMap("f", new SimpleImmutableEntry<>(true, null)),
                new MatchAll(new Source(1, 1, StringUtils.EMPTY)));
        assertEquals("NestedQuery@1:2[a.b.{f=true=null}[MatchAll@1:2[]]]", q.toString());
    }
}
