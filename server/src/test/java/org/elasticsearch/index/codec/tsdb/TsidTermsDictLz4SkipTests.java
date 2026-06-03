/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormatFactory;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class TsidTermsDictLz4SkipTests extends ESTestCase {

    private static final String TSID_FIELD = "_tsid";
    private static final int TSID_VALUE_COUNT = 4096;
    private static final int TSID_BYTES = 16;

    private static final String[] CLUSTER_NAMES = {
        "prod-eu-west",
        "prod-us-east",
        "prod-ap-southeast",
        "staging-eu-west",
        "staging-us-east" };

    private static final String[] REGIONS = { "eu-west-1", "us-east-1", "us-west-2", "ap-southeast-2", "eu-central-1" };

    public void testTsidValuesAreRoundTripIdenticalAcrossLz4SkipFlag() throws IOException {
        final List<BytesRef> tsids = randomDistinctBytes(random(), TSID_VALUE_COUNT, TSID_BYTES);

        try (Directory withLz4 = new ByteBuffersDirectory(); Directory skipLz4 = new ByteBuffersDirectory()) {
            writeIndex(withLz4, tsids, false);
            writeIndex(skipLz4, tsids, true);

            final List<BytesRef> lz4Read = readTsidValues(withLz4);
            final List<BytesRef> rawRead = readTsidValues(skipLz4);

            assertEquals("tsid value count must match", lz4Read.size(), rawRead.size());
            for (int i = 0; i < lz4Read.size(); i++) {
                assertEquals("tsid value at position " + i + " must round-trip identically", lz4Read.get(i), rawRead.get(i));
            }
        }
    }

    public void testWriterStampsLegacyVersionWhenLz4SkipIsDisabled() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            writeSingleSegmentNoCompound(dir, false);
            assertEquals(TSDBDocValuesFormatConfig.VERSION_SEPARATE_SKIPLIST, readDataFileVersion(dir));
        }
    }

    public void testWriterStampsRawFlagVersionWhenLz4SkipIsEnabled() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            writeSingleSegmentNoCompound(dir, true);
            assertEquals(TSDBDocValuesFormatConfig.VERSION_TERMS_DICT_RAW_FLAG, readDataFileVersion(dir));
        }
    }

    public void testForceMergeProducesValidIndexWhenSkippingLz4OnTsid() throws IOException {
        final List<BytesRef> tsids = randomDistinctBytes(random(), TSID_VALUE_COUNT, TSID_BYTES);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeIndexWithMultipleSegments(dir, tsids, true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                final SortedDocValues sdv = reader.leaves().get(0).reader().getSortedDocValues(TSID_FIELD);
                assertNotNull(sdv);
                final Set<BytesRef> seen = new HashSet<>();
                int docId;
                while ((docId = sdv.nextDoc()) != SortedDocValues.NO_MORE_DOCS) {
                    seen.add(BytesRef.deepCopyOf(sdv.lookupOrd(sdv.ordValue())));
                    assertTrue(docId >= 0);
                }
                assertEquals("expected one occurrence of each tsid value after force merge", tsids.size(), seen.size());
            }
        }
    }

    public void testTermsDictBytesAcrossCodecVariants() throws IOException {
        final IndexVersion v = IndexVersion.current();
        final List<BytesRef> tsids = realisticTsids(TSID_VALUE_COUNT, v);

        final DocValuesFormat es819Lz4 = ES819TSDBDocValuesFormatFactory.createDocValuesFormat(v, false, false, true, false);
        final DocValuesFormat es819Raw = ES819TSDBDocValuesFormatFactory.createDocValuesFormat(v, false, false, true, true);
        final DocValuesFormat es95Lz4 = ES95TSDBDocValuesFormatFactory.get(false, false, true, false);
        final DocValuesFormat es95Raw = ES95TSDBDocValuesFormatFactory.get(false, false, true, true);

        final long es819Lz4Bytes = measureTermsDictBytes(tsids, es819Lz4);
        final long es819RawBytes = measureTermsDictBytes(tsids, es819Raw);
        final long es95Lz4Bytes = measureTermsDictBytes(tsids, es95Lz4);
        final long es95RawBytes = measureTermsDictBytes(tsids, es95Raw);

        final String report = "_tsid termsDataLength for "
            + TSID_VALUE_COUNT
            + " tsids: ES819+LZ4="
            + es819Lz4Bytes
            + " ES819+RAW="
            + es819RawBytes
            + " (delta="
            + (es819Lz4Bytes - es819RawBytes)
            + ") ES95+LZ4="
            + es95Lz4Bytes
            + " ES95+RAW="
            + es95RawBytes
            + " (delta="
            + (es95Lz4Bytes - es95RawBytes)
            + ")";
        logger.info(report);

        assertTrue("ES819: raw should be <= LZ4: " + report, es819RawBytes <= es819Lz4Bytes);
        assertTrue("ES95: raw should be <= LZ4: " + report, es95RawBytes <= es95Lz4Bytes);
        assertEquals("Raw block sizes must match across codecs (same per-block format): " + report, es819RawBytes, es95RawBytes);
    }

    private static void writeIndex(final Directory dir, final List<BytesRef> tsids, final boolean skipLz4) throws IOException {
        final IndexWriterConfig cfg = new IndexWriterConfig().setCodec(newCodec(skipLz4));
        try (IndexWriter writer = new IndexWriter(dir, cfg)) {
            for (BytesRef tsid : tsids) {
                final Document doc = new Document();
                doc.add(new SortedDocValuesField(TSID_FIELD, tsid));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static void writeIndexWithMultipleSegments(final Directory dir, final List<BytesRef> tsids, final boolean skipLz4)
        throws IOException {
        final Codec codec = newCodec(skipLz4);
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, new IndexWriterConfig().setCodec(codec))) {
            for (int i = 0; i < tsids.size(); i++) {
                final Document doc = new Document();
                doc.add(new SortedDocValuesField(TSID_FIELD, tsids.get(i)));
                writer.addDocument(doc);
                if (i % 256 == 0) {
                    writer.commit();
                }
            }
            writer.forceMerge(1);
        }
    }

    private static void writeSingleSegmentNoCompound(final Directory dir, final boolean skipLz4) throws IOException {
        final IndexWriterConfig cfg = new IndexWriterConfig().setCodec(newCodec(skipLz4)).setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(dir, cfg)) {
            final Document doc = new Document();
            doc.add(new SortedDocValuesField(TSID_FIELD, new BytesRef(new byte[] { 1, 2, 3 })));
            writer.addDocument(doc);
            writer.forceMerge(1);
        }
    }

    private static Codec newCodec(final boolean skipLz4) {
        final DocValuesFormat format = new ES819Version3TSDBDocValuesFormat(false, false, false, skipLz4);
        return new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return format;
            }
        };
    }

    private static List<BytesRef> readTsidValues(final Directory dir) throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            for (var leaf : reader.leaves()) {
                final SegmentReader segmentReader = (SegmentReader) leaf.reader();
                final SortedDocValues sdv = segmentReader.getSortedDocValues(TSID_FIELD);
                if (sdv == null) {
                    continue;
                }
                int docId;
                while ((docId = sdv.nextDoc()) != SortedDocValues.NO_MORE_DOCS) {
                    assertTrue(docId >= 0);
                    values.add(BytesRef.deepCopyOf(sdv.lookupOrd(sdv.ordValue())));
                }
            }
        }
        return values;
    }

    private static int readDataFileVersion(final Directory dir) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals(1, reader.leaves().size());
            final SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
            final SegmentInfo info = segmentReader.getSegmentInfo().info;
            String dvdName = null;
            for (String name : dir.listAll()) {
                if (name.endsWith(".dvd")) {
                    assertNull("expected exactly one .dvd file", dvdName);
                    dvdName = name;
                }
            }
            assertNotNull(".dvd file must exist", dvdName);
            final String afterSegment = IndexFileNames.stripSegmentName(dvdName);
            final int dot = afterSegment.lastIndexOf('.');
            final String suffix = afterSegment.charAt(0) == '_' ? afterSegment.substring(1, dot) : "";
            try (IndexInput in = dir.openInput(dvdName, IOContext.DEFAULT)) {
                return CodecUtil.checkIndexHeader(
                    in,
                    "ES819TSDBDocValuesData",
                    TSDBDocValuesFormatConfig.VERSION_START,
                    TSDBDocValuesFormatConfig.VERSION_CURRENT,
                    info.getId(),
                    suffix
                );
            }
        }
    }

    private static long measureTermsDictBytes(final List<BytesRef> tsids, final DocValuesFormat format) throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            writeTsidIndexWithFormat(dir, tsids, format);
            return readTermsDictBytes(dir);
        }
    }

    private static void writeTsidIndexWithFormat(final Directory dir, final List<BytesRef> tsids, final DocValuesFormat format)
        throws IOException {
        final Codec codec = new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(final String field) {
                return format;
            }
        };
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec))) {
            for (BytesRef tsid : tsids) {
                final Document doc = new Document();
                doc.add(new SortedDocValuesField(TSID_FIELD, tsid));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static long readTermsDictBytes(final Directory dir) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals(1, reader.leaves().size());
            final SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
            final int tsidFieldNumber = segmentReader.getFieldInfos().fieldInfo(TSID_FIELD).number;
            final DocValuesProducer dvProducer = segmentReader.getDocValuesReader();
            final AbstractTSDBDocValuesProducer tsdbProducer = (AbstractTSDBDocValuesProducer) unwrapPerFieldProducer(dvProducer);
            return tsdbProducer.sorted.get(tsidFieldNumber).termsDictEntry.termsDataLength;
        }
    }

    private static DocValuesProducer unwrapPerFieldProducer(final DocValuesProducer dvReader) {
        assertTrue(
            "expected XPerFieldDocValuesFormat.FieldsReader, got " + dvReader.getClass(),
            dvReader instanceof XPerFieldDocValuesFormat.FieldsReader
        );
        final Map<String, DocValuesProducer> formats = ((XPerFieldDocValuesFormat.FieldsReader) dvReader).getFormats();
        assertEquals("expected exactly one inner producer", 1, formats.size());
        return formats.values().iterator().next();
    }

    private static List<BytesRef> randomDistinctBytes(final Random random, final int count, final int width) {
        final List<BytesRef> values = new ArrayList<>(count);
        final Set<BytesRef> seen = new HashSet<>();
        while (values.size() < count) {
            final byte[] bytes = new byte[width];
            random.nextBytes(bytes);
            final BytesRef ref = new BytesRef(bytes);
            if (seen.add(ref)) {
                values.add(ref);
            }
        }
        values.sort(BytesRef::compareTo);
        return values;
    }

    private static List<BytesRef> realisticTsids(final int count, final IndexVersion indexVersion) {
        final Random random = new Random(42L);
        final Set<BytesRef> seen = new HashSet<>();
        final List<BytesRef> out = new ArrayList<>(count);
        while (out.size() < count) {
            final String cluster = CLUSTER_NAMES[random.nextInt(CLUSTER_NAMES.length)];
            final String region = REGIONS[random.nextInt(REGIONS.length)];
            final int hostId = random.nextInt(10_000);
            final String hostName = "host-" + Integer.toHexString(hostId);
            final String hostIp = (10 + (hostId >>> 16) % 245)
                + "."
                + ((hostId >>> 8) & 0xFF)
                + "."
                + (hostId & 0xFF)
                + "."
                + random.nextInt(255);
            final int cpu = random.nextInt(16);
            final BytesRef tsid = TsidBuilder.newBuilder()
                .addStringDimension("cluster.name", cluster)
                .addStringDimension("region", region)
                .addStringDimension("host.name", hostName)
                .addStringDimension("host.ip", hostIp)
                .addIntDimension("attributes.cpu", cpu)
                .buildTsid(indexVersion);
            if (seen.add(tsid)) {
                out.add(tsid);
            }
        }
        out.sort(BytesRef::compareTo);
        return out;
    }
}
