/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Base class for the tests that read the values of a stub segment, which they attach from a Parquet file that a test wrote. */
abstract class StubSegmentTestCase extends ESTestCase {

    static final String FILE = "repository://lake/flat.parquet";

    /** Runs a check against the leaf reader of the opened stub segment. */
    interface LeafCheck {
        void check(LeafReader leaf) throws IOException;
    }

    /** Attaches the file as one stub segment with the given fields, opens it in the context of an engine, and runs the check. */
    static void withSegment(BytesParquetObject file, List<ColumnBinding> bindings, LeafCheck check) throws IOException {
        ParquetFileInfo footer = ParquetFooterReader.read(file);
        List<String> columns = bindings.stream().map(ColumnBinding::parquetColumnPath).toList();
        ExternalFilePiece piece = footer.piece(0, footer.rowGroups().size(), 0, columns);
        try (Directory dir = ParquetTestFiles.newStubDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig().setCodec(new ParquetAttachCodec()).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                writer.addIndexes(new ExternalParquetCodecReader(bindings, List.of(piece)));
                writer.commit();
            }
            // the files that the segment reads come from the context that is active while it opens
            ParquetObjects objects = (location, length) -> {
                assertEquals(FILE, location);
                assertEquals(file.length(), length);
                return file;
            };
            ParquetReadContext.with(objects, () -> {
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    check.check(reader.leaves().get(0).reader());
                }
                return null;
            });
        }
    }

    /** The encodings of the data pages of a chunk, read straight from the page headers. */
    static Set<String> pageEncodings(byte[] file, ColumnSnapshot column, int rowGroup) throws IOException {
        int position = (int) column.offsets()[rowGroup];
        int end = position + (int) column.compressedSizes()[rowGroup];
        Set<String> encodings = new HashSet<>();
        while (position < end) {
            ByteArrayInputStream in = new ByteArrayInputStream(file, position, end - position);
            PageHeader header = Util.readPageHeader(in);
            int headerLength = (end - position) - in.available();
            if (header.getType() == PageType.DATA_PAGE) {
                encodings.add(header.getData_page_header().getEncoding().name());
            } else if (header.getType() == PageType.DATA_PAGE_V2) {
                encodings.add(header.getData_page_header_v2().getEncoding().name());
            }
            position += headerLength + header.getCompressed_page_size();
        }
        return encodings;
    }
}
