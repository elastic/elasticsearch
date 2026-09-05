/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;

import java.util.Arrays;
import java.util.List;

public class TermBasedFieldTypeTests extends FieldTypeTestCase {

    // termsQuery on an unsorted input must produce the same TermInSetQuery as the sorted input,
    // because termsQuery pre-sorts both inputs before building PrefixCodedTerms.
    public void testTermsQueryPreSortsTerms() {
        MappedFieldType ft = new KeywordFieldMapper.KeywordFieldType("field");

        List<String> unsorted = Arrays.asList("zzz", "aaa", "mmm", "bbb");
        List<String> sorted   = Arrays.asList("aaa", "bbb", "mmm", "zzz");

        Query fromUnsorted = ft.termsQuery(unsorted, MOCK_CONTEXT);
        Query fromSorted   = ft.termsQuery(sorted,   MOCK_CONTEXT);

        assertEquals(fromSorted, fromUnsorted);
    }

    // A single-term termsQuery should still work correctly.
    public void testTermsQuerySingleTerm() {
        MappedFieldType ft = new KeywordFieldMapper.KeywordFieldType("field");

        Query result   = ft.termsQuery(List.of("only"), MOCK_CONTEXT);
        Query expected = new TermInSetQuery("field", new BytesRef[] { new BytesRef("only") });

        assertEquals(expected, result);
    }

    // Pre-sorting must be stable across duplicate terms (TermInSetQuery deduplicates internally).
    public void testTermsQueryWithDuplicates() {
        MappedFieldType ft = new KeywordFieldMapper.KeywordFieldType("field");

        Query withDups    = ft.termsQuery(Arrays.asList("b", "a", "b", "a"), MOCK_CONTEXT);
        Query withoutDups = ft.termsQuery(Arrays.asList("a", "b"),           MOCK_CONTEXT);

        assertEquals(withoutDups, withDups);
    }
}
