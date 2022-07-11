/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedField;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.RootFlattenedMappedFieldType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RootFlattenedFieldTypeTests extends FieldTypeTestCase {

    private static RootFlattenedMappedFieldType createDefaultFieldType() {
        return new RootFlattenedMappedFieldType(true, true, Collections.emptyMap(), false, false);
    }

    public void testValueForDisplay() {
        RootFlattenedMappedFieldType ft = createDefaultFieldType();

        String fieldValue = "{ \"key\": \"value\" }";
        BytesRef storedValue = new BytesRef(fieldValue);
        assertEquals(fieldValue, ft.valueForDisplay(storedValue));
    }

    public void testTermQuery() {
        RootFlattenedMappedFieldType ft = createDefaultFieldType();

        Query expected = new TermQuery(new Term("field", "value"));
        assertEquals(expected, ft.termQuery("field", "value", null));

        expected = AutomatonQueries.caseInsensitiveTermQuery(new Term("field", "Value"));
        assertEquals(expected, ft.termQueryCaseInsensitive("field", "Value", null));

        RootFlattenedMappedFieldType unsearchable = new RootFlattenedMappedFieldType(false, true, Collections.emptyMap(), false, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("field", "field", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testExistsQuery() {
        RootFlattenedMappedFieldType ft = new RootFlattenedMappedFieldType(true, false, Collections.emptyMap(), false, false);
        assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, new BytesRef("field"))), ft.existsQuery("field", null));

        RootFlattenedMappedFieldType withDv = new RootFlattenedMappedFieldType(true, true, Collections.emptyMap(), false, false);
        assertEquals(new FieldExistsQuery("field"), withDv.existsQuery("field", null));
    }

    public void testFuzzyQuery() {
        RootFlattenedMappedFieldType ft = createDefaultFieldType();

        Query expected = new FuzzyQuery(new Term("field", "value"), 2, 1, 50, true);
        Query actual = ft.fuzzyQuery("field", "value", Fuzziness.fromEdits(2), 1, 50, true, MOCK_CONTEXT);
        assertEquals(expected, actual);

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.fuzzyQuery(
                "field",
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
        RootFlattenedMappedFieldType ft = createDefaultFieldType();

        TermRangeQuery expected = new TermRangeQuery("field", new BytesRef("lower"), new BytesRef("upper"), false, false);
        assertEquals(expected, ft.rangeQuery("field", "lower", "upper", false, false, MOCK_CONTEXT));

        expected = new TermRangeQuery("field", new BytesRef("lower"), new BytesRef("upper"), true, true);
        assertEquals(expected, ft.rangeQuery("field", "lower", "upper", true, true, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.rangeQuery("field", "lower", "upper", true, true, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[range] queries on [text] or [keyword] fields cannot be executed when " + "'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }

    public void testRegexpQuery() {
        RootFlattenedMappedFieldType ft = createDefaultFieldType();

        Query expected = new RegexpQuery(new Term("field", "val.*"));
        Query actual = ft.regexpQuery("field", "val.*", 0, 0, 10, null, MOCK_CONTEXT);
        assertEquals(expected, actual);

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.regexpQuery("field", "val.*", randomInt(10), 0, randomInt(10) + 1, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testWildcardQuery() {
        RootFlattenedMappedFieldType ft = createDefaultFieldType();

        Query expected = new WildcardQuery(new Term("field", new BytesRef("valu*")));
        assertEquals(expected, ft.wildcardQuery("field", "valu*", null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.wildcardQuery("field", "valu*", null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[wildcard] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testFetchSourceValue() throws IOException {
        Map<String, Object> sourceValue = Map.of("key", "value");
        MappedField mappedField = new MappedField("field", createDefaultFieldType());

        assertEquals(List.of(sourceValue), fetchSourceValue(mappedField, sourceValue));
        assertEquals(List.of(), fetchSourceValue(mappedField, null));
    }
}
