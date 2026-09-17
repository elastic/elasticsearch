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
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene50;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

/**
 * Lucene 5.0 compound file format
 *
 * <p>Files:
 *
 * <ul>
 *   <li><code>.cfs</code>: An optional "virtual" file consisting of all the other index files for
 *       systems that frequently run out of file handles.
 *   <li><code>.cfe</code>: The "virtual" compound file's entry table holding all entries in the
 *       corresponding .cfs file.
 * </ul>
 *
 * <p>Description:
 *
 * <ul>
 *   <li>Compound (.cfs) --&gt; Header, FileData <sup>FileCount</sup>, Footer
 *   <li>Compound Entry Table (.cfe) --&gt; Header, FileCount, &lt;FileName, DataOffset,
 *       DataLength&gt; <sup>FileCount</sup>
 *   <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *   <li>FileCount --&gt; {@link DataOutput#writeVInt VInt}
 *   <li>DataOffset,DataLength,Checksum --&gt; {@link DataOutput#writeLong UInt64}
 *   <li>FileName --&gt; {@link DataOutput#writeString String}
 *   <li>FileData --&gt; raw file data
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 * </ul>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>FileCount indicates how many files are contained in this compound file. The entry table
 *       that follows has that many entries.
 *   <li>Each directory entry contains a long pointer to the start of this file's data section, the
 *       files length, and a String with that file's name.
 * </ul>
 */
public final class Lucene50CompoundFormat extends CompoundFormat {

    /** Extension of compound file */
    static final String DATA_EXTENSION = "cfs";

    /** Extension of compound file entries */
    static final String ENTRIES_EXTENSION = "cfe";

    static final String DATA_CODEC = "Lucene50CompoundData";
    static final String ENTRY_CODEC = "Lucene50CompoundEntries";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;

    /** Sole constructor. */
    public Lucene50CompoundFormat() {}

    @Override
    public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si) throws IOException {
        return new Lucene50CompoundReader(dir, si);
    }

    @Override
    public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
        throw new UnsupportedOperationException("Old formats can't be used for writing");
    }
}
