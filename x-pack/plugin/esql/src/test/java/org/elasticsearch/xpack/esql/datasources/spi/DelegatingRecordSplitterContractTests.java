/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class DelegatingRecordSplitterContractTests extends RecordSplitterContractTests {

    @Override
    protected RecordSplitter newSplitter() {
        return new DelegatingRecordSplitter(new NewlineSegmentableFormatReader());
    }

    @Override
    protected Fixtures fixtures() {
        return new Fixtures(bytes("\n"), true, false, false, false);
    }

    public void testSegmentableFormatReaderHookReturnsDelegatingSplitter() {
        assertThat(new NewlineSegmentableFormatReader().recordSplitter(), instanceOf(DelegatingRecordSplitter.class));
    }

    public void testFindLastRecordBoundaryOverrideIsPreserved() throws IOException {
        byte[] input = bytes("xxone\ntwo\n");
        RecordSplitter splitter = new DelegatingRecordSplitter(new OverridingFindLastRecordBoundaryReader());

        assertEquals(2, splitter.findLastRecordBoundary(input, 0, input.length));
        assertEquals(4, splitter.findLastRecordBoundary(input, 2, input.length - 2));
    }

    private static class NewlineSegmentableFormatReader implements SegmentableFormatReader, NoConfigFormatReader {

        @Override
        public long findNextRecordBoundary(InputStream stream) throws IOException {
            long consumed = 0;
            int next;
            while ((next = stream.read()) != -1) {
                consumed++;
                if (next == '\n') {
                    return consumed;
                }
            }
            return -1;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            throw new UnsupportedOperationException("test reader");
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new UnsupportedOperationException("test reader");
        }

        @Override
        public String formatName() {
            return "newline";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    private static class OverridingFindLastRecordBoundaryReader extends NewlineSegmentableFormatReader {
        @Override
        public int findLastRecordBoundary(byte[] buf, int length) {
            return 2;
        }
    }
}
