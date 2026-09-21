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
package org.elasticsearch.index.codec.lucene86;

import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Lucene 8.6 point format, which encodes dimensional values in a block KD-tree structure for fast
 * 1D range and N dimensional shape intersection filtering. See <a
 * href="https://www.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf">this paper</a> for
 * details.
 *
 * <p>Data is stored across three files
 *
 * <ul>
 *   <li>A .kdm file that records metadata about the fields, such as numbers of dimensions or
 *       numbers of bytes per dimension.
 *   <li>A .kdi file that stores inner nodes of the tree.
 *   <li>A .kdd file that stores leaf nodes, where most of the data lives.
 * </ul>
 *
 * See <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=173081898">this
 * wiki</a> for detailed data structures of the three files.
 *
 */
public class Lucene86PointsFormat extends PointsFormat {

    static final String DATA_CODEC_NAME = "Lucene86PointsFormatData";
    static final String INDEX_CODEC_NAME = "Lucene86PointsFormatIndex";
    static final String META_CODEC_NAME = "Lucene86PointsFormatMeta";

    /** Filename extension for the leaf blocks */
    public static final String DATA_EXTENSION = "kdd";

    /** Filename extension for the index per field */
    public static final String INDEX_EXTENSION = "kdi";

    /** Filename extension for the meta per field */
    public static final String META_EXTENSION = "kdm";

    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;

    /** Sole constructor */
    public Lucene86PointsFormat() {}

    @Override
    public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        throw new UnsupportedOperationException("Old codecs may only be used for reading");
    }

    @Override
    public PointsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene86PointsReader(state);
    }
}
