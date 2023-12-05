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
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene50;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Lucene 5.0 Segment info format.
 * @deprecated Only for reading old 5.0-6.0 segments
 */
@Deprecated
public class Lucene50SegmentInfoFormat extends SegmentInfoFormat {

    public Lucene50SegmentInfoFormat() {}

    @Override
    public SegmentInfo read(Directory dir, String segment, byte[] segmentID, IOContext context) throws IOException {
        final String fileName = IndexFileNames.segmentFileName(segment, "", Lucene50SegmentInfoFormat.SI_EXTENSION);
        try (ChecksumIndexInput input = EndiannessReverserUtil.openChecksumInput(dir, fileName, context)) {
            Throwable priorE = null;
            SegmentInfo si = null;
            try {
                CodecUtil.checkIndexHeader(
                    input,
                    Lucene50SegmentInfoFormat.CODEC_NAME,
                    Lucene50SegmentInfoFormat.VERSION_START,
                    Lucene50SegmentInfoFormat.VERSION_CURRENT,
                    segmentID,
                    ""
                );
                final Version version = Version.fromBits(input.readInt(), input.readInt(), input.readInt());

                final int docCount = input.readInt();
                if (docCount < 0) {
                    throw new CorruptIndexException("invalid docCount: " + docCount, input);
                }
                final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;

                final Map<String, String> diagnostics = input.readMapOfStrings();
                final Set<String> files = input.readSetOfStrings();
                final Map<String, String> attributes = input.readMapOfStrings();

                si = new SegmentInfo(
                    dir,
                    version,
                    null,
                    segment,
                    docCount,
                    isCompoundFile,
                    false,
                    null,
                    diagnostics,
                    segmentID,
                    attributes,
                    null
                );
                si.setFiles(files);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(input, priorE);
            }
            return si;
        }
    }

    @Override
    public void write(Directory dir, SegmentInfo si, IOContext ioContext) {
        throw new UnsupportedOperationException("this codec can only be used for reading");
    }

    /** File extension used to store {@link SegmentInfo}. */
    public static final String SI_EXTENSION = "si";
    static final String CODEC_NAME = "Lucene50SegmentInfo";
    static final int VERSION_SAFE_MAPS = 1;
    static final int VERSION_START = VERSION_SAFE_MAPS;
    static final int VERSION_CURRENT = VERSION_SAFE_MAPS;
}
