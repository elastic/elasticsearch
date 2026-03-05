/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.LegacyBaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.GraalVMThreadsFilter;

import java.util.Arrays;
import java.util.function.LongSupplier;

@ThreadLeakFilters(filters = { GraalVMThreadsFilter.class })
public class Lucene70DocValuesFormatTests extends LegacyBaseDocValuesFormatTestCase {

    private final Codec codec = TestUtil.alwaysDocValuesFormat(new Lucene70DocValuesFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }

    /**
     * Patched copy of the base class method that adds a missing {@code writer.commit()} after
     * deleting docs so that {@code DirectoryReader.open(dir)} sees the deletions.
     * This can be removed when the upstream Lucene fix is integrated (Lucene 10.5+).
     */
    private void doTestSortedNumericsVsStoredFieldsPatched(LongSupplier counts, LongSupplier values) throws Exception {
        assumeFalse(
            "Remove this method and the overrides that call it; the upstream Lucene bug has been fixed in 10.5",
            IndexVersion.current().luceneVersion().onOrAfter(org.apache.lucene.util.Version.fromBits(10, 5, 0))
        );
        Directory dir = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);

        int numDocs = atLeast(300);
        assert numDocs > 256;
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", Integer.toString(i), org.apache.lucene.document.Field.Store.NO));

            int valueCount = (int) counts.getAsLong();
            long[] valueArray = new long[valueCount];
            for (int j = 0; j < valueCount; j++) {
                long value = values.getAsLong();
                valueArray[j] = value;
                doc.add(new SortedNumericDocValuesField("dv", value));
            }
            Arrays.sort(valueArray);
            for (int j = 0; j < valueCount; j++) {
                doc.add(new StoredField("stored", Long.toString(valueArray[j])));
            }
            writer.addDocument(doc);
            if (random().nextInt(31) == 0) {
                writer.commit();
            }
        }

        // delete some docs
        int numDeletions = random().nextInt(numDocs / 10);
        for (int i = 0; i < numDeletions; i++) {
            int id = random().nextInt(numDocs);
            writer.deleteDocuments(new Term("id", Integer.toString(id)));
        }
        writer.commit();
        try (DirectoryReader reader = maybeWrapWithMergingReader(DirectoryReader.open(dir))) {
            TestUtil.checkReader(reader);
            compareStoredFieldWithSortedNumericsDV(reader, "stored", "dv");
        }
        writer.forceMerge(numDocs / 256);
        try (DirectoryReader reader = maybeWrapWithMergingReader(DirectoryReader.open(dir))) {
            TestUtil.checkReader(reader);
            compareStoredFieldWithSortedNumericsDV(reader, "stored", "dv");
        }
        IOUtils.close(writer, dir);
    }

    @Override
    public void testSortedNumericsSingleValuedVsStoredFields() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestSortedNumericsVsStoredFieldsPatched(() -> 1, random()::nextLong);
        }
    }

    @Override
    public void testSortedNumericsSingleValuedMissingVsStoredFields() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestSortedNumericsVsStoredFieldsPatched(() -> random().nextBoolean() ? 0 : 1, random()::nextLong);
        }
    }

    @Override
    public void testSortedNumericsMultipleValuesVsStoredFields() throws Exception {
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestSortedNumericsVsStoredFieldsPatched(() -> TestUtil.nextLong(random(), 0, 50), random()::nextLong);
        }
    }

    @Override
    public void testSortedNumericsFewUniqueSetsVsStoredFields() throws Exception {
        final long[] uniqueValues = new long[TestUtil.nextInt(random(), 2, 6)];
        for (int i = 0; i < uniqueValues.length; ++i) {
            uniqueValues[i] = random().nextLong();
        }
        int numIterations = atLeast(1);
        for (int i = 0; i < numIterations; i++) {
            doTestSortedNumericsVsStoredFieldsPatched(
                () -> TestUtil.nextLong(random(), 0, 6),
                () -> uniqueValues[random().nextInt(uniqueValues.length)]
            );
        }
    }
}
