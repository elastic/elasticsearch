/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.matchonlytext.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType;
import org.elasticsearch.xpack.matchonlytext.mapper.MatchOnlyTextFieldMapper.MatchOnlyTextFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MatchOnlyTextFieldTypeTests extends FieldTypeTestCase {

    public void testTermQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        assertEquals(new ConstantScoreQuery(new TermQuery(new Term("field", "foo"))), ft.termQuery("foo", null));
        assertEquals(AutomatonQueries.caseInsensitiveTermQuery(new Term("field", "fOo")), ft.termQueryCaseInsensitive("fOo", null));

        MappedFieldType unsearchable = new TextFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("bar", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testTermsQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef("foo"));
        terms.add(new BytesRef("bar"));
        assertEquals(new TermInSetQuery("field", terms), ft.termsQuery(Arrays.asList("foo", "bar"), null));

        MappedFieldType unsearchable = new TextFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.termsQuery(Arrays.asList("foo", "bar"), null)
        );
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testRangeQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        assertEquals(
            new TermRangeQuery("field", BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("bar"), true, false),
            ft.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_QSC)
        );

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[range] queries on [text] or [keyword] fields cannot be executed when " + "'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }

    public void testRegexpQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        assertEquals(new RegexpQuery(new Term("field", "foo.*")), ft.regexpQuery("foo.*", 0, 0, 10, null, MOCK_QSC));

        MappedFieldType unsearchable = new TextFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.regexpQuery("foo.*", 0, 0, 10, null, MOCK_QSC)
        );
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.regexpQuery("foo.*", randomInt(10), 0, randomInt(10) + 1, null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testFuzzyQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        assertEquals(
            new FuzzyQuery(new Term("field", "foo"), 2, 1, 50, true),
            ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_QSC)
        );

        MappedFieldType unsearchable = new TextFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_QSC)
        );
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.fuzzyQuery("foo", Fuzziness.AUTO, randomInt(10) + 1, randomInt(10) + 1, randomBoolean(), MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals("[fuzzy] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testFetchSourceValue() throws IOException {
        MatchOnlyTextFieldType fieldType = new MatchOnlyTextFieldType("field");

        assertEquals(List.of("value"), fetchSourceValue(fieldType, "value"));
        assertEquals(List.of("42"), fetchSourceValue(fieldType, 42L));
        assertEquals(List.of("true"), fetchSourceValue(fieldType, true));
    }

}
