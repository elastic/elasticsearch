/*
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

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene86;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Lucene 8.6 Segment info format.
 *
 * <p>Files:
 *
 * <ul>
 *   <li><code>.si</code>: Header, SegVersion, SegSize, IsCompoundFile, Diagnostics, Files,
 *       Attributes, IndexSort, Footer
 * </ul>
 *
 * Data types:
 *
 * <ul>
 *   <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *   <li>SegSize --&gt; {@link DataOutput#writeInt Int32}
 *   <li>SegVersion --&gt; {@link DataOutput#writeString String}
 *   <li>SegMinVersion --&gt; {@link DataOutput#writeString String}
 *   <li>Files --&gt; {@link DataOutput#writeSetOfStrings Set&lt;String&gt;}
 *   <li>Diagnostics,Attributes --&gt; {@link DataOutput#writeMapOfStrings Map&lt;String,String&gt;}
 *   <li>IsCompoundFile --&gt; {@link DataOutput#writeByte Int8}
 *   <li>IndexSort --&gt; {@link DataOutput#writeVInt Int32} count, followed by {@code count}
 *       SortField
 *   <li>SortField --&gt; {@link DataOutput#writeString String} sort class, followed by a per-sort
 *       bytestream (see {@link SortFieldProvider#readSortField(DataInput)})
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 * </ul>
 *
 * Field Descriptions:
 *
 * <ul>
 *   <li>SegVersion is the code version that created the segment.
 *   <li>SegMinVersion is the minimum code version that contributed documents to the segment.
 *   <li>SegSize is the number of documents contained in the segment index.
 *   <li>IsCompoundFile records whether the segment is written as a compound file or not. If this is
 *       -1, the segment is not a compound file. If it is 1, the segment is a compound file.
 *   <li>The Diagnostics Map is privately written by {@link IndexWriter}, as a debugging aid, for
 *       each segment it creates. It includes metadata like the current Lucene version, OS, Java
 *       version, why the segment was created (merge, flush, addIndexes), etc.
 *   <li>Files is a list of files referred to by this segment.
 * </ul>
 *
 * @see SegmentInfos
 * @lucene.experimental
 */
public class Lucene86SegmentInfoFormat extends SegmentInfoFormat {

    /** File extension used to store {@link SegmentInfo}. */
    public static final String SI_EXTENSION = "si";

    static final String CODEC_NAME = "Lucene86SegmentInfo";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;

    /** Sole constructor. */
    public Lucene86SegmentInfoFormat() {}

    @Override
    public SegmentInfo read(Directory dir, String segment, byte[] segmentID, IOContext context) throws IOException {
        final String fileName = IndexFileNames.segmentFileName(segment, "", SI_EXTENSION);
        try (ChecksumIndexInput input = EndiannessReverserUtil.openChecksumInput(dir, fileName, context)) {
            Throwable priorE = null;
            SegmentInfo si = null;
            try {
                CodecUtil.checkIndexHeader(input, CODEC_NAME, VERSION_START, VERSION_CURRENT, segmentID, "");
                si = parseSegmentInfo(dir, input, segment, segmentID);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(input, priorE);
            }
            return si;
        }
    }

    private SegmentInfo parseSegmentInfo(Directory dir, DataInput input, String segment, byte[] segmentID) throws IOException {
        final Version version = Version.fromBits(input.readInt(), input.readInt(), input.readInt());
        byte hasMinVersion = input.readByte();
        final Version minVersion;
        switch (hasMinVersion) {
            case 0:
                minVersion = null;
                break;
            case 1:
                minVersion = Version.fromBits(input.readInt(), input.readInt(), input.readInt());
                break;
            default:
                throw new CorruptIndexException("Illegal boolean value " + hasMinVersion, input);
        }

        final int docCount = input.readInt();
        if (docCount < 0) {
            throw new CorruptIndexException("invalid docCount: " + docCount, input);
        }
        final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;

        final Map<String, String> diagnostics = input.readMapOfStrings();
        final Set<String> files = input.readSetOfStrings();
        final Map<String, String> attributes = input.readMapOfStrings();

        int numSortFields = input.readVInt();
        Sort indexSort;
        if (numSortFields > 0) {
            SortField[] sortFields = new SortField[numSortFields];
            for (int i = 0; i < numSortFields; i++) {
                String name = input.readString();
                sortFields[i] = SortFieldProvider.forName(name).readSortField(input);
            }
            indexSort = new Sort(sortFields);
        } else if (numSortFields < 0) {
            throw new CorruptIndexException("invalid index sort field count: " + numSortFields, input);
        } else {
            indexSort = null;
        }

        SegmentInfo si = new SegmentInfo(
            dir,
            version,
            minVersion,
            segment,
            docCount,
            isCompoundFile,
            false,
            null,
            diagnostics,
            segmentID,
            attributes,
            indexSort
        );
        si.setFiles(files);
        return si;
    }

    @Override
    public void write(Directory dir, SegmentInfo si, IOContext ioContext) throws IOException {
        throw new UnsupportedOperationException("Old formats can't be used for writing");
    }
}
