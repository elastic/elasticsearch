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
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.tests.index.BaseStoredFieldsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ES93BloomFilterStoredFieldsFormatTests extends BaseStoredFieldsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Override
    protected Codec getCodec() {
        return new AssertingCodec() {
            @Override
            public StoredFieldsFormat storedFieldsFormat() {
                var bloomFilterSizeInKb = atLeast(2);
                return new ES93BloomFilterStoredFieldsFormat(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    "",
                    TestUtil.getDefaultCodec().storedFieldsFormat(),
                    ByteSizeValue.ofKb(bloomFilterSizeInKb),
                    IdFieldMapper.NAME
                );
            }
        };
    }

    @Override
    protected void addRandomFields(Document doc) {

    }

    public void testBloomFilterFieldIsNotStoredAndBloomFilterCanBeChecked() throws IOException {
        try (var directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            conf.setCodec(getCodec());
            conf.setMergePolicy(newLogMergePolicy());
            // We want to have at most 1 segment
            conf.setMaxBufferedDocs(200);
            // The stored fields reader that can be accessed through a StandardDirectoryReader wraps
            // the ES93BloomFilterStoredFieldsFormat.Reader. Thus, we need to open it directly from
            // the segment info codec and if we use compound files we would need to obtain a compound
            // file directory. For simplicity, we just use regular files.
            conf.setUseCompoundFile(false);
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
            conf.setCodec(new AssertingCodec() {
                @Override
                public StoredFieldsFormat storedFieldsFormat() {
                    var bloomFilterSizeInKb = atLeast(2);
                    return new ES93BloomFilterStoredFieldsFormat(
                        BigArrays.NON_RECYCLING_INSTANCE,
                        "",
                        TestUtil.getDefaultCodec().storedFieldsFormat(),
                        ByteSizeValue.ofKb(bloomFilterSizeInKb),
                        IdFieldMapper.NAME
                    ) {
                        @Override
                        int getBloomFilterSizeInBits() {
                            if (randomBloomFilterSizes) {
                                // Use different power of 2 values so we rebuild the bloom filter from the _id terms
                                var bloomFilterSizeInBytes = ByteSizeValue.ofKb(1).getBytes() << atLeast(5);

                                return ES93BloomFilterStoredFieldsFormat.closestPowerOfTwoBloomFilterSizeInBits(
                                    ByteSizeValue.ofBytes(bloomFilterSizeInBytes)
                                );
                            }
                            return super.getBloomFilterSizeInBits();
                        }
                    };
                }
            });
            conf.setMergePolicy(new FilterMergePolicy(newLogMergePolicy()) {
                @Override
                public boolean useCompoundFile(SegmentInfos infos, SegmentCommitInfo mergedInfo, MergeContext mergeContext) {
                    return false;
                }
            });
            conf.setMaxBufferedDocs(10);
            // The stored fields reader that can be accessed through a StandardDirectoryReader wraps
            // the ES93BloomFilterStoredFieldsFormat.Reader. Thus, we need to open it directly from
            // the segment info codec and if we use compound files we would need to obtain a compound
            // file directory. For simplicity, we just use regular files.
            conf.setUseCompoundFile(false);
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
            var id = getBytesRefFromString(UUIDs.randomBase64UUID());
            indexedIds.add(id);
            doc.add(new StringField(IdFieldMapper.NAME, id, Field.Store.YES));
            doc.add(new StringField("host", "host-" + i, Field.Store.YES));
            doc.add(new LongField("counter", i, Field.Store.YES));
            writer.addDocument(doc);
        }
        return indexedIds;
    }

    private void assertBloomFilterTestsPositiveForExistingDocs(IndexWriter writer, List<BytesRef> indexedIds) throws IOException {
        try (var directoryReader = StandardDirectoryReader.open(writer)) {
            for (LeafReaderContext leaf : directoryReader.leaves()) {
                try (ES93BloomFilterStoredFieldsFormat.BloomFilterProvider fieldReader = getBloomFilterProvider(leaf)) {
                    var bloomFilter = fieldReader.getBloomFilter();
                    // the bloom filter reader is null only if the _id field is not stored during indexing
                    assertThat(bloomFilter, is(not(nullValue())));

                    for (BytesRef indexedId : indexedIds) {
                        assertThat(bloomFilter.mayContainTerm(IdFieldMapper.NAME, indexedId), is(true));
                    }
                    assertThat(bloomFilter.mayContainTerm(IdFieldMapper.NAME, getBytesRefFromString("random")), is(false));

                    assertThat(bloomFilter.mayContainTerm(IdFieldMapper.NAME, getBytesRefFromString("12345")), is(false));
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

    private static BytesRef getBytesRefFromString(String random) {
        return new BytesRef(random.getBytes(StandardCharsets.UTF_8));
    }

    private ES93BloomFilterStoredFieldsFormat.BloomFilterProvider getBloomFilterProvider(LeafReaderContext leafReaderContext)
        throws IOException {
        LeafReader reader = leafReaderContext.reader();
        var fieldInfos = reader.getFieldInfos();
        assertThat(reader, is(instanceOf(SegmentReader.class)));
        SegmentReader segmentReader = (SegmentReader) reader;
        SegmentInfo si = segmentReader.getSegmentInfo().info;

        var storedFieldsReader = si.getCodec().storedFieldsFormat().fieldsReader(si.dir, si, fieldInfos, IOContext.DEFAULT);
        assertThat(storedFieldsReader, is(instanceOf(ES93BloomFilterStoredFieldsFormat.BloomFilterProvider.class)));
        return ((ES93BloomFilterStoredFieldsFormat.BloomFilterProvider) storedFieldsReader);
    }
}
