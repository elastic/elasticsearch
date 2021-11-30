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
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene50;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Collection;

/**
 * Lucene 5.0 live docs format
 * @deprecated
 */
@Deprecated
public final class Lucene50LiveDocsFormat extends LiveDocsFormat {

    public Lucene50LiveDocsFormat() {}

    /** extension of live docs */
    private static final String EXTENSION = "liv";

    /** codec of live docs */
    private static final String CODEC_NAME = "Lucene50LiveDocs";

    /** supported version range */
    private static final int VERSION_START = 0;
    private static final int VERSION_CURRENT = VERSION_START;

    @Override
    public Bits readLiveDocs(Directory dir, SegmentCommitInfo info, IOContext context) throws IOException {
        long gen = info.getDelGen();
        String name = IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, gen);
        final int length = info.info.maxDoc();
        try (ChecksumIndexInput input = EndiannessReverserUtil.openChecksumInput(dir, name, context)) {
            Throwable priorE = null;
            try {
                CodecUtil.checkIndexHeader(
                    input,
                    CODEC_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    info.info.getId(),
                    Long.toString(gen, Character.MAX_RADIX)
                );
                long data[] = new long[FixedBitSet.bits2words(length)];
                for (int i = 0; i < data.length; i++) {
                    data[i] = input.readLong();
                }
                FixedBitSet fbs = new FixedBitSet(data, length);
                if (fbs.length() - fbs.cardinality() != info.getDelCount()) {
                    throw new CorruptIndexException(
                        "bits.deleted=" + (fbs.length() - fbs.cardinality()) + " info.delcount=" + info.getDelCount(),
                        input
                    );
                }
                return fbs.asReadOnlyBits();
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(input, priorE);
            }
        }
        throw new AssertionError();
    }

    @Override
    public void writeLiveDocs(Bits bits, Directory dir, SegmentCommitInfo info, int newDelCount, IOContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void files(SegmentCommitInfo info, Collection<String> files) {
        if (info.hasDeletions()) {
            files.add(IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, info.getDelGen()));
        }
    }
}
