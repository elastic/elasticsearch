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
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.lucene50;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Collection;

/**
 * Lucene 5.0 live docs format
 *
 * <p>The .liv file is optional, and only exists when a segment contains deletions.
 *
 * <p>Although per-segment, this file is maintained exterior to compound segment files.
 *
 * <p>Deletions (.liv) --&gt; IndexHeader,Generation,Bits
 *
 * <ul>
 *   <li>SegmentHeader --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *   <li>Bits --&gt; &lt;{@link DataOutput#writeLong Int64}&gt; <sup>LongCount</sup>
 * </ul>
 */
public final class Lucene50LiveDocsFormat extends LiveDocsFormat {

    /** extension of live docs */
    private static final String EXTENSION = "liv";

    /** codec of live docs */
    private static final String CODEC_NAME = "Lucene50LiveDocs";

    /** supported version range */
    private static final int VERSION_START = 0;

    private static final int VERSION_CURRENT = VERSION_START;

    /** Sole constructor. */
    public Lucene50LiveDocsFormat() {}

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

                FixedBitSet fbs = readFixedBitSet(input, length);

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

    private FixedBitSet readFixedBitSet(IndexInput input, int length) throws IOException {
        long[] data = new long[FixedBitSet.bits2words(length)];
        for (int i = 0; i < data.length; i++) {
            data[i] = input.readLong();
        }
        return new FixedBitSet(data, length);
    }

    /**
     * Note: although this format is only used on older versions, we need to keep the write logic in
     * addition to the read logic. When we delete documents that live in an older segment, we write to
     * the live docs for that segment.
     */
    @Override
    public void writeLiveDocs(Bits bits, Directory dir, SegmentCommitInfo info, int newDelCount, IOContext context) throws IOException {
        long gen = info.getNextDelGen();
        String name = IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, gen);
        int delCount;
        try (IndexOutput output = EndiannessReverserUtil.createOutput(dir, name, context)) {

            CodecUtil.writeIndexHeader(output, CODEC_NAME, VERSION_CURRENT, info.info.getId(), Long.toString(gen, Character.MAX_RADIX));

            delCount = writeBits(output, bits);

            CodecUtil.writeFooter(output);
        }
        if (delCount != info.getDelCount() + newDelCount) {
            throw new CorruptIndexException(
                "bits.deleted=" + delCount + " info.delcount=" + info.getDelCount() + " newdelcount=" + newDelCount,
                name
            );
        }
    }

    private int writeBits(IndexOutput output, Bits bits) throws IOException {
        int delCount = 0;
        final int longCount = FixedBitSet.bits2words(bits.length());
        for (int i = 0; i < longCount; ++i) {
            long currentBits = 0;
            for (int j = i << 6, end = Math.min(j + 63, bits.length() - 1); j <= end; ++j) {
                if (bits.get(j)) {
                    currentBits |= 1L << j; // mod 64
                } else {
                    delCount += 1;
                }
            }
            output.writeLong(currentBits);
        }
        return delCount;
    }

    @Override
    public void files(SegmentCommitInfo info, Collection<String> files) throws IOException {
        if (info.hasDeletions()) {
            files.add(IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, info.getDelGen()));
        }
    }
}
