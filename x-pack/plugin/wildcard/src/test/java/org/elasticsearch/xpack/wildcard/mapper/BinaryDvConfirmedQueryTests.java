/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class BinaryDvConfirmedQueryTests extends ESTestCase {

    /**
     * The fuzzy automaton is already UTF-8 byte-level, so wrapping its {@code automaton} in a fresh {@code ByteRunAutomaton} converts it
     * a second time and mis-matches non-ASCII values. It must run the pre-compiled {@code runAutomaton}.
     */
    public void testFuzzyMatchesNonAsciiValues() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (String value : new String[] { "héllo", "hello", "hèllo", "world" }) {
                    final Document document = new Document();
                    final BytesRef encoded = MultiValuedBinaryDocValuesField.IntegratedCount.encode(List.of(new BytesRef(value)));
                    document.add(new BinaryDocValuesField("field", encoded));
                    writer.addDocument(document);
                }
                try (DirectoryReader reader = writer.getReader()) {
                    final IndexSearcher searcher = new IndexSearcher(reader);
                    final FuzzyQuery fuzzy = new FuzzyQuery(new Term("field", "héllo"), 1, 0, 50, true);
                    final Query query = BinaryDvConfirmedQuery.fromFuzzyQuery(Queries.ALL_DOCS_INSTANCE, "field", "héllo", fuzzy, false);
                    // "world" is more than one edit away; the other three are within one edit of the search term
                    assertThat(searcher.count(query), equalTo(3));
                }
            }
        }
    }

}
