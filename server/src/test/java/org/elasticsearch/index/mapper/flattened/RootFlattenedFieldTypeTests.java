/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.RootFlattenedFieldType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RootFlattenedFieldTypeTests extends FieldTypeTestCase {

    private static RootFlattenedFieldType createDefaultFieldType(int ignoreAbove) {
        return new RootFlattenedFieldType("field", true, true, Collections.emptyMap(), false, false, ignoreAbove);
    }

    public void testValueForDisplay() {
        RootFlattenedFieldType ft = createDefaultFieldType(Integer.MAX_VALUE);

        String fieldValue = "{ \"key\": \"value\" }";
        BytesRef storedValue = new BytesRef(fieldValue);
        assertEquals(fieldValue, ft.valueForDisplay(storedValue));
    }

    public void testTermQuery() {
        RootFlattenedFieldType ft = createDefaultFieldType(Integer.MAX_VALUE);

        Query expected = new TermQuery(new Term("field", "value"));
        assertEquals(expected, ft.termQuery("value", null));

        expected = AutomatonQueries.caseInsensitiveTermQuery(new Term("field", "Value"));
        assertEquals(expected, ft.termQueryCaseInsensitive("Value", null));

        RootFlattenedFieldType unsearchable = new RootFlattenedFieldType(
            "field",
            false,
            true,
            Collections.emptyMap(),
            false,
            false,
            Integer.MAX_VALUE
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("field", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testExistsQuery() {
        RootFlattenedFieldType ft = new RootFlattenedFieldType(
            "field",
            true,
            false,
            Collections.emptyMap(),
            false,
            false,
            Integer.MAX_VALUE
        );
        assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, new BytesRef("field"))), ft.existsQuery(null));

        RootFlattenedFieldType withDv = new RootFlattenedFieldType(
            "field",
            true,
            true,
            Collections.emptyMap(),
            false,
            false,
            Integer.MAX_VALUE
        );
        assertEquals(new FieldExistsQuery("field"), withDv.existsQuery(null));
    }

    public void testFuzzyQuery() {
        RootFlattenedFieldType ft = createDefaultFieldType(Integer.MAX_VALUE);

        Query expected = new FuzzyQuery(new Term("field", "value"), 2, 1, 50, true);
        Query actual = ft.fuzzyQuery("value", Fuzziness.fromEdits(2), 1, 50, true, MOCK_CONTEXT);
        assertEquals(expected, actual);

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.fuzzyQuery(
                "value",
                Fuzziness.AUTO,
                randomInt(10) + 1,
                randomInt(10) + 1,
                randomBoolean(),
                MOCK_CONTEXT_DISALLOW_EXPENSIVE
            )
        );
        assertEquals("[fuzzy] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testRangeQuery() {
        RootFlattenedFieldType ft = createDefaultFieldType(Integer.MAX_VALUE);

        TermRangeQuery expected = new TermRangeQuery("field", new BytesRef("lower"), new BytesRef("upper"), false, false);
        assertEquals(expected, ft.rangeQuery("lower", "upper", false, false, MOCK_CONTEXT));

        expected = new TermRangeQuery("field", new BytesRef("lower"), new BytesRef("upper"), true, true);
        assertEquals(expected, ft.rangeQuery("lower", "upper", true, true, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.rangeQuery("lower", "upper", true, true, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[range] queries on [text] or [keyword] fields cannot be executed when " + "'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }

    public void testRegexpQuery() {
        RootFlattenedFieldType ft = createDefaultFieldType(Integer.MAX_VALUE);

        Query expected = new RegexpQuery(new Term("field", "val.*"));
        Query actual = ft.regexpQuery("val.*", 0, 0, 10, null, MOCK_CONTEXT);
        assertEquals(expected, actual);

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.regexpQuery("val.*", randomInt(10), 0, randomInt(10) + 1, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testWildcardQuery() {
        RootFlattenedFieldType ft = createDefaultFieldType(Integer.MAX_VALUE);

        Query expected = new WildcardQuery(new Term("field", new BytesRef("valu*")));
        assertEquals(expected, ft.wildcardQuery("valu*", null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.wildcardQuery("valu*", null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[wildcard] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testFetchSourceValue() throws IOException {
        Map<String, Object> sourceValue = Map.of("key", "value");
        RootFlattenedFieldType ft = createDefaultFieldType(Integer.MAX_VALUE);

        assertEquals(List.of(sourceValue), fetchSourceValue(ft, sourceValue));
        assertEquals(List.of(), fetchSourceValue(ft, null));
    }

    public void testFetchSourceValueWithIgnoreAbove() throws IOException {
        Map<String, Object> sourceValue = Map.of("key", "test ignore above");

        assertEquals(List.of(), fetchSourceValue(createDefaultFieldType(10), sourceValue));
        assertEquals(List.of(sourceValue), fetchSourceValue(createDefaultFieldType(20), sourceValue));
    }

    public void testFetchSourceValueWithList() throws IOException {
        Map<String, Object> sourceValue = Map.of("key1", List.of("one", "two", "three"));

        assertEquals(List.of(Map.of("key1", List.of("one", "two"))), fetchSourceValue(createDefaultFieldType(3), sourceValue));
    }

    public void testFetchSourceValueWithMultipleFields() throws IOException {
        Map<String, Object> sourceValue = Map.of(
            "key1",
            "test",
            "key2",
            List.of("one", "two", "three"),
            "key3",
            "hi",
            "key4",
            List.of("the quick brown fox", "jumps over the lazy dog")
        );

        assertEquals(
            List.of(Map.of("key2", List.of("one", "two"), "key3", "hi")),
            fetchSourceValue(createDefaultFieldType(3), sourceValue)
        );
    }

    public void testFetchSourceValueWithMixedFieldTypes() throws IOException {
        Map<String, Object> sourceValue = Map.of("key1", List.of("one", 1, "two", 2));

        assertEquals(List.of(Map.of("key1", List.of("one", 1, "two", 2))), fetchSourceValue(createDefaultFieldType(3), sourceValue));
    }

    public void testFetchSourceValueWithNonString() throws IOException {
        Map<String, Object> sourceValue = Map.of("key1", List.of(100, 200), "key2", 50L, "key3", new Tuple<>(10, 100));

        assertEquals(
            List.of(Map.of("key1", List.of(100, 200), "key2", 50L, "key3", new Tuple<>(10, 100))),
            fetchSourceValue(createDefaultFieldType(3), sourceValue)
        );
    }

    public void testFetchSourceValueFilterStringsOnly() throws IOException {
        Map<String, Object> sourceValue = Map.of("key1", List.of("the quick brown", 1_234_567, "jumps over", 2_456));

        assertEquals(List.of(Map.of("key1", List.of(1_234_567, 2_456))), fetchSourceValue(createDefaultFieldType(8), sourceValue));
    }
}
