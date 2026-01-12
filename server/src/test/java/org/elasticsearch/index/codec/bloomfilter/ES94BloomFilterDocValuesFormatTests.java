/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ES94BloomFilterDocValuesFormatTests extends ESTestCase {
    public void testBloomFilterFieldIsNotStoredAndBloomFilterCanBeChecked() throws IOException {
        try (var directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            var bloomFilterSizeInKb = atLeast(2);
            conf.setCodec(
                new TestCodec(
                    new ES94BloomFilterDocValuesFormat(
                        BigArrays.NON_RECYCLING_INSTANCE,
                        ByteSizeValue.ofKb(bloomFilterSizeInKb),
                        IdFieldMapper.NAME
                    )
                )
            );
            conf.setMergePolicy(newLogMergePolicy());
            // We want to have at most 1 segment
            conf.setMaxBufferedDocs(200);
            conf.setUseCompoundFile(randomBoolean());
            // We don't use the RandomIndexWriter because we want to control the settings so we get
            // deterministic test runs
            try (IndexWriter writer = new IndexWriter(directory, conf)) {
                List<BytesRef> indexedIds = indexDocs(writer);

                assertBloomFilterTestsPositiveForExistingDocs(writer, indexedIds);
            }
        }
    }

    public void testBloomFilterMerges() throws IOException {
        try (var directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            var randomBloomFilterSizes = random().nextBoolean();
            var bloomFilterSizeInKb = atLeast(2);
            conf.setCodec(
                new TestCodec(
                    new ES94BloomFilterDocValuesFormat(
                        BigArrays.NON_RECYCLING_INSTANCE,
                        ByteSizeValue.ofKb(bloomFilterSizeInKb),
                        IdFieldMapper.NAME
                    ) {
                        @Override
                        int getBloomFilterSizeInBits() {
                            if (randomBloomFilterSizes) {
                                // Use different power of 2 values so we rebuild the bloom filter from the _id terms
                                var bloomFilterSizeInBytes = ByteSizeValue.ofKb(1).getBytes() << atLeast(5);

                                return ES94BloomFilterDocValuesFormat.closestPowerOfTwoBloomFilterSizeInBits(
                                    ByteSizeValue.ofBytes(bloomFilterSizeInBytes)
                                );
                            }
                            return super.getBloomFilterSizeInBits();
                        }
                    }

                )
            );
            conf.setMergePolicy(new FilterMergePolicy(newLogMergePolicy()) {
                @Override
                public boolean useCompoundFile(SegmentInfos infos, SegmentCommitInfo mergedInfo, MergeContext mergeContext) {
                    return false;
                }
            });
            conf.setMaxBufferedDocs(10);
            conf.setUseCompoundFile(randomBoolean());
            // We don't use the RandomIndexWriter because we want to control the settings so we get
            // deterministic test runs
            try (IndexWriter writer = new IndexWriter(directory, conf)) {
                List<BytesRef> indexedIds = indexDocs(writer);

                writer.forceMerge(1);

                assertBloomFilterTestsPositiveForExistingDocs(writer, indexedIds);
            }
        }
    }

    private static List<BytesRef> indexDocs(IndexWriter writer) throws IOException {
        List<BytesRef> indexedIds = new ArrayList<>();
        var docCount = atLeast(50);
        for (int i = 0; i < docCount; i++) {
            Document doc = new Document();
            var id = UUIDs.randomBase64UUID();
            indexedIds.add(new BytesRef(id));
            doc.add(new IdField(id));
            doc.add(new StringField("host", "host-" + i, Field.Store.YES));
            doc.add(new LongField("counter", i, Field.Store.YES));
            writer.addDocument(doc);
        }
        return indexedIds;
    }

    private void assertBloomFilterTestsPositiveForExistingDocs(IndexWriter writer, List<BytesRef> indexedIds) throws IOException {
        try (var directoryReader = StandardDirectoryReader.open(writer)) {
            for (LeafReaderContext leaf : directoryReader.leaves()) {
                try (var bloomFilter = getBloomFilter(leaf)) {
                    // the bloom filter reader is null only if the _id field is not stored during indexing
                    assertThat(bloomFilter, is(not(nullValue())));

                    for (BytesRef indexedId : indexedIds) {
                        assertThat(bloomFilter.mayContainValue(IdFieldMapper.NAME, indexedId), is(true));
                    }
                    assertThat(bloomFilter.mayContainValue(IdFieldMapper.NAME, new BytesRef("random")), is(false));

                    assertThat(bloomFilter.mayContainValue(IdFieldMapper.NAME, new BytesRef("12345")), is(false));
                }
            }

            var storedFields = directoryReader.storedFields();
            for (int docId = 0; docId < indexedIds.size(); docId++) {
                var document = storedFields.document(docId);
                // The _id field is not actually stored, just used to build the bloom filter
                assertThat(document.get(IdFieldMapper.NAME), nullValue());
                assertThat(document.get("host"), not(nullValue()));
                assertThat(document.get("host"), is(equalTo("host-" + docId)));
                assertThat(document.get("counter"), not(nullValue()));
                assertThat(document.getField("counter").storedValue().getLongValue(), is(equalTo((long) docId)));
            }
        }
    }

    private BloomFilter getBloomFilter(LeafReaderContext leafReaderContext) throws IOException {
        LeafReader reader = leafReaderContext.reader();
        var binaryDocValues = reader.getBinaryDocValues(IdFieldMapper.NAME);

        assertThat(binaryDocValues, is(instanceOf(BloomFilter.class)));
        return (BloomFilter) binaryDocValues;
    }

    static class TestCodec extends AssertingCodec {
        private final ES94BloomFilterDocValuesFormat bloomFilterDocValuesFormat;

        TestCodec(ES94BloomFilterDocValuesFormat bloomFilterDocValuesFormat) {
            this.bloomFilterDocValuesFormat = bloomFilterDocValuesFormat;
        }

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            if (field.equals(IdFieldMapper.NAME)) {
                return bloomFilterDocValuesFormat;
            }
            return super.getDocValuesFormatForField(field);
        }
    }

    // The test relies on the Id field being actually indexed so we can
    // rebuild the bloom filter from the terms without having to rely
    // on the synthetic id terms. That's why we use a new custom type
    // instead of SyntheticIdField. Additionally, we cannot use a regular
    // StringField since we expect it to have doc values configured.
    static class IdField extends Field {

        private static final FieldType TYPE;

        static {
            TYPE = new FieldType();
            TYPE.setIndexOptions(IndexOptions.DOCS);
            TYPE.setDocValuesType(DocValuesType.BINARY);
            TYPE.setTokenized(false);
            TYPE.setOmitNorms(true);
            TYPE.setStored(false);
            TYPE.freeze();
        }

        private final BytesRef binaryValue;

        IdField(String id) {
            super(IdFieldMapper.NAME, TYPE);
            this.binaryValue = new BytesRef(id);
        }

        @Override
        public InvertableType invertableType() {
            return InvertableType.BINARY;
        }

        @Override
        public BytesRef binaryValue() {
            return binaryValue;
        }
    }
}
