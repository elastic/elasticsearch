/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.RootFlattenedFieldType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RootFlattenedFieldTypeTests extends FieldTypeTestCase {

    private static RootFlattenedFieldType createDefaultFieldType() {
        return new RootFlattenedFieldType("field", true, true, Collections.emptyMap(), false, false);
    }

    public void testValueForDisplay() {
        RootFlattenedFieldType ft = createDefaultFieldType();

        String fieldValue = "{ \"key\": \"value\" }";
        BytesRef storedValue = new BytesRef(fieldValue);
        assertEquals(fieldValue, ft.valueForDisplay(storedValue));
    }

    public void testTermQuery() {
        RootFlattenedFieldType ft = createDefaultFieldType();

        Query expected = new TermQuery(new Term("field", "value"));
        assertEquals(expected, ft.termQuery("value", null));

        expected = AutomatonQueries.caseInsensitiveTermQuery(new Term("field", "Value"));
        assertEquals(expected, ft.termQueryCaseInsensitive("Value", null));

        RootFlattenedFieldType unsearchable = new RootFlattenedFieldType("field", false, true, Collections.emptyMap(), false, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("field", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testExistsQuery() {
        RootFlattenedFieldType ft = new RootFlattenedFieldType("field", true, false, Collections.emptyMap(), false, false);
        assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, new BytesRef("field"))), ft.existsQuery(null));

        RootFlattenedFieldType withDv = new RootFlattenedFieldType("field", true, true, Collections.emptyMap(), false, false);
        assertEquals(new DocValuesFieldExistsQuery("field"), withDv.existsQuery(null));
    }

    public void testFuzzyQuery() {
        RootFlattenedFieldType ft = createDefaultFieldType();

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
        RootFlattenedFieldType ft = createDefaultFieldType();

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
        RootFlattenedFieldType ft = createDefaultFieldType();

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
        RootFlattenedFieldType ft = createDefaultFieldType();

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
        RootFlattenedFieldType ft = createDefaultFieldType();

        assertEquals(List.of(sourceValue), fetchSourceValue(ft, sourceValue));
        assertEquals(List.of(), fetchSourceValue(ft, null));
    }
}
