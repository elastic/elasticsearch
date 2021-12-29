/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2020 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.search.suggest.fst.BytesRefSorter;
import org.apache.lucene.search.suggest.fst.ExternalRefSorter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.OfflineSorter;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;

/**
 * Copy of {@link ExternalRefSorter} that uses a {@link OfflineSorter.ByteSequencesWriter} to write the temporary file.
 *
 * TODO: Move to Lucene
 */
class XExternalRefSorter implements BytesRefSorter, Closeable {
    private final CompressingOfflineSorter sorter;
    private CompressingOfflineSorter.Writer writer;
    private IndexOutput input;
    private String sortedFileName;

    /**
     * Will buffer all sequences to a temporary file and then sort (all on-disk).
     */
    XExternalRefSorter(CompressingOfflineSorter sorter) throws IOException {
        this.sorter = sorter;
        this.input = sorter.getDirectory().createTempOutput(sorter.getTempFileNamePrefix(), "RefSorterRaw", IOContext.DEFAULT);
        this.writer = sorter.getWriter(input, -1);
    }

    @Override
    public void add(BytesRef utf8) throws IOException {
        if (writer == null) {
            throw new IllegalStateException();
        }
        writer.write(utf8);
    }

    @Override
    public BytesRefIterator iterator() throws IOException {
        if (sortedFileName == null) {
            closeWriter();

            boolean success = false;
            try {
                sortedFileName = sorter.sort(input.getName());
                success = true;
            } finally {
                if (success) {
                    sorter.getDirectory().deleteFile(input.getName());
                } else {
                    deleteFilesIgnoringExceptions(sorter.getDirectory(), input.getName());
                }
            }

            input = null;
        }

        OfflineSorter.ByteSequencesReader reader = sorter.getReader(
            sorter.getDirectory().openChecksumInput(sortedFileName, IOContext.READONCE),
            sortedFileName
        );
        return new ByteSequenceIterator(reader);
    }

    private void closeWriter() throws IOException {
        if (writer != null) {
            CodecUtil.writeFooter(writer.out);
            writer.close();
            writer = null;
        }
    }

    /**
     * Removes any written temporary files.
     */
    @Override
    public void close() throws IOException {
        try {
            closeWriter();
        } finally {
            if (input == null) {
                deleteFilesIgnoringExceptions(sorter.getDirectory(), input == null ? null : input.getName(), sortedFileName);
            }
        }
    }

    /**
     * Iterate over byte refs in a file.
     */
    // TODO: this class is a bit silly ... sole purpose is to "remove" Closeable from what #iterator returns:
    static class ByteSequenceIterator implements BytesRefIterator {
        private final OfflineSorter.ByteSequencesReader reader;
        private BytesRef scratch;

        ByteSequenceIterator(OfflineSorter.ByteSequencesReader reader) {
            this.reader = reader;
        }

        @Override
        public BytesRef next() throws IOException {
            boolean success = false;
            try {
                scratch = reader.next();
                if (scratch == null) {
                    reader.close();
                }
                success = true;
                return scratch;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(reader);
                }
            }
        }
    }

    @Override
    public Comparator<BytesRef> getComparator() {
        return sorter.getComparator();
    }

    private static void deleteFilesIgnoringExceptions(Directory dir, String... files) {
        for (String name : files) {
            if (name != null) {
                try {
                    dir.deleteFile(name);
                } catch (Throwable ignored) {
                    // ignore
                }
            }
        }
    }
}
