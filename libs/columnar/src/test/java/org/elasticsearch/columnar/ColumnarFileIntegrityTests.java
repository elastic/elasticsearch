/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.hamcrest.Matchers.containsString;

/** Every file the format writes is checksummed: one flipped byte in any of them is reported, never read as data. */
public class ColumnarFileIntegrityTests extends ESTestCase {

    private static final Set<String> EXTENSIONS = Set.of("cnd", "cna", "cnl", "cnn", "cnm", "cns");

    public void testACorruptByteInAnyFileIsReported() throws IOException {
        try (Directory written = new ByteBuffersDirectory()) {
            write(written);
            final List<String> files = new ArrayList<>();
            for (String file : written.listAll()) {
                if (EXTENSIONS.contains(file.substring(file.lastIndexOf('.') + 1))) {
                    files.add(file);
                }
            }
            assertEquals("one file of each kind", EXTENSIONS.size(), files.size());
            for (String victim : files) {
                try (Directory copy = new ByteBuffersDirectory()) {
                    for (String file : written.listAll()) {
                        copy.copyFrom(written, file, file, IOContext.DEFAULT);
                    }
                    flipAByte(copy, victim);
                    final Exception e = expectThrows(Exception.class, () -> {
                        try (DirectoryReader reader = DirectoryReader.open(copy)) {
                            for (LeafReaderContext leaf : reader.leaves()) {
                                ((CodecReader) leaf.reader()).checkIntegrity();
                            }
                        }
                    });
                    assertTrue(
                        victim + " reported as " + e,
                        e instanceof CorruptIndexException
                            || e instanceof IndexFormatTooNewException
                            || e instanceof IndexFormatTooOldException
                            // a flipped length in a header reads past the end of the file
                            || e instanceof EOFException
                    );
                }
            }
        }
    }

    public void testAnUnknownFramingByteIsRejectedBehindAValidChecksum() throws IOException {
        final byte unknown = randomFrom((byte) 2, (byte) 3, (byte) 127, (byte) -1);
        try (Directory dir = new ByteBuffersDirectory()) {
            write(dir);
            final SegmentCommitInfo segment = SegmentInfos.readLatestCommit(dir).info(0);
            final String metaFile = metaFileOf(dir);
            final String segmentSuffix = IndexFileNames.stripExtension(IndexFileNames.stripSegmentName(metaFile)).substring(1);
            final int fieldNumberAndTypeLength = Integer.BYTES + Byte.BYTES;
            final byte[] bytes;
            final int framingOffset;
            try (IndexInput in = dir.openInput(metaFile, IOContext.DEFAULT)) {
                ColumnarCodecUtil.checkHeader(in, ColumNARDocValuesFormat.META_CODEC, segment.info.getId(), segmentSuffix);
                framingOffset = Math.toIntExact(in.getFilePointer()) + fieldNumberAndTypeLength;
                bytes = new byte[Math.toIntExact(in.length())];
                in.seek(0);
                in.readBytes(bytes, 0, bytes.length);
            }
            bytes[framingOffset] = unknown;
            dir.deleteFile(metaFile);
            try (IndexOutput out = dir.createOutput(metaFile, IOContext.DEFAULT)) {
                out.writeBytes(bytes, 0, bytes.length - CodecUtil.footerLength());
                CodecUtil.writeFooter(out);
            }

            final CorruptIndexException e = expectThrows(CorruptIndexException.class, () -> DirectoryReader.open(dir).close());
            assertThat(e.getMessage(), containsString("unknown string column framing [" + unknown + "]"));
        }
    }

    private static String metaFileOf(Directory dir) throws IOException {
        for (String file : dir.listAll()) {
            if (file.endsWith("." + ColumNARDocValuesFormat.META_EXTENSION)) {
                return file;
            }
        }
        throw new AssertionError("no metadata file among " + List.of(dir.listAll()));
    }

    /** Plain values of many lengths, several slots a document and nulls, so every file holds more than its header. */
    private static void write(Directory dir) throws IOException {
        final FieldType type = columnarBinaryFieldType();
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING)).setUseCompoundFile(false);
        iwc.getMergePolicy().setNoCFSRatio(0.0);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int d = 0; d < 3000; d++) {
                final List<BytesRef> slots = new ArrayList<>();
                for (int s = between(1, 3); s > 0; s--) {
                    slots.add(randomInt(9) == 0 ? null : new BytesRef("value-" + d + "-" + randomAlphaOfLength(between(0, 30))));
                }
                final Document doc = new Document();
                doc.add(new Field("field", BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(slots)), type));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static void flipAByte(Directory dir, String file) throws IOException {
        final byte[] bytes;
        try (IndexInput in = dir.openInput(file, IOContext.DEFAULT)) {
            bytes = new byte[Math.toIntExact(in.length())];
            in.readBytes(bytes, 0, bytes.length);
        }
        bytes[between(0, bytes.length - 1)] ^= (byte) (1 << between(0, 7));
        dir.deleteFile(file);
        try (IndexOutput out = dir.createOutput(file, IOContext.DEFAULT)) {
            out.writeBytes(bytes, 0, bytes.length);
        }
    }
}
