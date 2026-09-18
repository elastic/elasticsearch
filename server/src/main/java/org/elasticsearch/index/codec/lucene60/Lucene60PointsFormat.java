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
package org.elasticsearch.index.codec.lucene60;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Lucene 6.0 point format, which encodes dimensional values in a block KD-tree structure for fast
 * 1D range and N dimensional shape intersection filtering. See <a
 * href="https://www.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf">this paper</a> for
 * details.
 *
 * <p>This data structure is written as a series of blocks on disk, with an in-memory perfectly
 * balanced binary tree of split values referencing those blocks at the leaves.
 *
 * <p>The <code>.dim</code> file has both blocks and the index split values, for each field. The
 * file starts with {@link CodecUtil#writeIndexHeader}.
 *
 * <p>The blocks are written like this:
 *
 * <ul>
 *   <li>count (vInt)
 *   <li>delta-docID (vInt) <sup>count</sup> (delta coded docIDs, in sorted order)
 *   <li>packedValue<sup>count</sup> (the <code>byte[]</code> value of each dimension packed into a
 *       single <code>byte[]</code>)
 * </ul>
 *
 * <p>After all blocks for a field are written, then the index is written:
 *
 * <ul>
 *   <li>numDims (vInt)
 *   <li>maxPointsInLeafNode (vInt)
 *   <li>bytesPerDim (vInt)
 *   <li>count (vInt)
 *   <li>packed index (byte[])
 * </ul>
 *
 * <p>The packed index uses hierarchical delta and prefix coding to compactly encode the file
 * pointer for all leaf blocks, once the tree is traversed, as well as the split dimension and split
 * value for each inner node of the tree.
 *
 * <p>After all fields blocks + index data are written, {@link CodecUtil#writeFooter} writes the
 * checksum.
 *
 * <p>The <code>.dii</code> file records the file pointer in the <code>.dim</code> file where each
 * field's index data was written. It starts with {@link CodecUtil#writeIndexHeader}, then has:
 *
 * <ul>
 *   <li>fieldCount (vInt)
 *   <li>(fieldNumber (vInt), fieldFilePointer (vLong))<sup>fieldCount</sup>
 * </ul>
 *
 * <p>After all fields blocks + index data are written, {@link CodecUtil#writeFooter} writes the
 * checksum.
 *
 * @lucene.experimental
 */
public class Lucene60PointsFormat extends PointsFormat {

    static final String DATA_CODEC_NAME = "Lucene60PointsFormatData";
    static final String META_CODEC_NAME = "Lucene60PointsFormatMeta";

    /** Filename extension for the leaf blocks */
    public static final String DATA_EXTENSION = "dim";

    /** Filename extension for the index per field */
    public static final String INDEX_EXTENSION = "dii";

    static final int DATA_VERSION_START = 0;
    static final int DATA_VERSION_CURRENT = DATA_VERSION_START;

    static final int INDEX_VERSION_START = 0;
    static final int INDEX_VERSION_CURRENT = INDEX_VERSION_START;

    /** Sole constructor */
    public Lucene60PointsFormat() {}

    @Override
    public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        throw new UnsupportedOperationException("Old codecs may only be used for reading");
    }

    @Override
    public PointsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene60PointsReader(state);
    }
}
