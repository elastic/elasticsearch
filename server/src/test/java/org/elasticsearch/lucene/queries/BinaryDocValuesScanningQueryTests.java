/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.queries;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.index.mapper.BinaryDocValuesFormat.SEPARATE_COUNT;

/**
 * Runs every binary doc-values scanning query over one single-valued corpus and checks the match counts. The corpus is
 * written once with the {@code .counts} companion and once without (the {@code writeCounts} parameter), so the same
 * assertions cover both encodings the scanning queries must read as single-valued. It is sparse (some documents have no
 * value) so the whole-blob fast paths are disabled and every query goes through the shared scanning path.
 */
public class BinaryDocValuesScanningQueryTests extends ESTestCase {

    private static final String FIELD = "field";

    private final boolean writeCounts;

    /**
     * @param writeCounts whether each indexed value is written with its {@code .counts} companion, so the queries are
     *                    exercised against both the counts-present and counts-absent single-valued encodings
     */
    public BinaryDocValuesScanningQueryTests(@Name("writeCounts") boolean writeCounts) {
        this.writeCounts = writeCounts;
    }

    /** Runs the suite twice: once writing the {@code .counts} companion for each value and once omitting it. */
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    public void testTermQuery() throws IOException {
        assertMatchCount(new ScanningBinaryDocValuesTermQuery(FIELD, new BytesRef("research"), SEPARATE_COUNT), 1);
    }

    public void testPrefixQuery() throws IOException {
        assertMatchCount(new ScanningBinaryDocValuesPrefixQuery(FIELD, "re", false, SEPARATE_COUNT), 1);
    }

    public void testWildcardQuery() throws IOException {
        assertMatchCount(ScanningBinaryDocValuesAutomatonQuery.forWildcard(FIELD, "resea*", false, SEPARATE_COUNT), 1);
    }

    public void testRegexpQuery() throws IOException {
        assertMatchCount(
            new ScanningBinaryDocValuesRegexpQuery(
                FIELD,
                "re.*",
                RegExp.ALL,
                0,
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                SEPARATE_COUNT,
                null
            ),
            1
        );
    }

    public void testTermInSetQuery() throws IOException {
        assertMatchCount(
            new ScanningBinaryDocValuesTermInSetQuery(FIELD, List.of(new BytesRef("kibana"), new BytesRef("research")), SEPARATE_COUNT),
            2
        );
    }

    public void testContainsQuery() throws IOException {
        assertMatchCount(new BinaryDocValuesContainsTermQuery(FIELD, new BytesRef("search"), SEPARATE_COUNT), 3);
    }

    private void assertMatchCount(Query query, int expected) throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newIndexWriter(dir)) {
                indexValue(writer, "elasticsearch");
                writer.addDocument(new Document());
                indexValue(writer, "kibana");
                indexValue(writer, "research");
                writer.addDocument(new Document());
                indexValue(writer, "logstash");
                indexValue(writer, "searching");
                try (IndexReader reader = writer.getReader()) {
                    final IndexSearcher searcher = newSearcher(reader);
                    assertEquals(expected, searcher.count(query));
                }
            }
        }
    }

    private void indexValue(RandomIndexWriter writer, String value) throws IOException {
        final Document document = new Document();
        final MultiValuedBinaryDocValuesField.SeparateCount field = new MultiValuedBinaryDocValuesField.SeparateCount(
            FIELD,
            MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
        );
        field.add(new BytesRef(value.getBytes(StandardCharsets.UTF_8)));
        document.add(field);
        if (writeCounts) {
            document.add(NumericDocValuesField.indexedField(FIELD + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX, 1));
        }
        writer.addDocument(document);
    }

    private static RandomIndexWriter newIndexWriter(Directory dir) throws IOException {
        final IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
        return new RandomIndexWriter(random(), dir, iwc);
    }
}
