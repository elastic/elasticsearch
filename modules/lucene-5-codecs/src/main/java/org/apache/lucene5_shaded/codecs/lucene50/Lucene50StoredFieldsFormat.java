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
 */
package org.apache.lucene5_shaded.codecs.lucene50;


import java.io.IOException;
import java.util.Objects;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.StoredFieldsFormat;
import org.apache.lucene5_shaded.codecs.StoredFieldsReader;
import org.apache.lucene5_shaded.codecs.StoredFieldsWriter;
import org.apache.lucene5_shaded.codecs.compressing.CompressingStoredFieldsFormat;
import org.apache.lucene5_shaded.codecs.compressing.CompressingStoredFieldsIndexWriter;
import org.apache.lucene5_shaded.codecs.compressing.CompressionMode;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.index.StoredFieldVisitor;
import org.apache.lucene5_shaded.store.DataOutput;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.util.packed.PackedInts;

/**
 * Lucene 5.0 stored fields format.
 *
 * <p><b>Principle</b>
 * <p>This {@link StoredFieldsFormat} compresses blocks of documents in
 * order to improve the compression ratio compared to document-level
 * compression. It uses the <a href="http://code.google.com/p/lz4/">LZ4</a>
 * compression algorithm by default in 16KB blocks, which is fast to compress 
 * and very fast to decompress data. Although the default compression method 
 * that is used ({@link Mode#BEST_SPEED BEST_SPEED}) focuses more on speed than on 
 * compression ratio, it should provide interesting compression ratios
 * for redundant inputs (such as log files, HTML or plain text). For higher
 * compression, you can choose ({@link Mode#BEST_COMPRESSION BEST_COMPRESSION}), which uses 
 * the <a href="http://en.wikipedia.org/wiki/DEFLATE">DEFLATE</a> algorithm with 60KB blocks 
 * for a better ratio at the expense of slower performance. 
 * These two options can be configured like this:
 * <pre class="prettyprint">
 *   // the default: for high performance
 *   indexWriterConfig.setCodec(new Lucene54Codec(Mode.BEST_SPEED));
 *   // instead for higher performance (but slower):
 *   // indexWriterConfig.setCodec(new Lucene54Codec(Mode.BEST_COMPRESSION));
 * </pre>
 * <p><b>File formats</b>
 * <p>Stored fields are represented by two files:
 * <ol>
 * <li><a name="field_data"></a>
 * <p>A fields data file (extension <tt>.fdt</tt>). This file stores a compact
 * representation of documents in compressed blocks of 16KB or more. When
 * writing a segment, documents are appended to an in-memory <tt>byte[]</tt>
 * buffer. When its size reaches 16KB or more, some metadata about the documents
 * is flushed to disk, immediately followed by a compressed representation of
 * the buffer using the
 * <a href="http://code.google.com/p/lz4/">LZ4</a>
 * <a href="http://fastcompression.blogspot.fr/2011/05/lz4-explained.html">compression format</a>.</p>
 * <p>Here is a more detailed description of the field data file format:</p>
 * <ul>
 * <li>FieldData (.fdt) --&gt; &lt;Header&gt;, PackedIntsVersion, &lt;Chunk&gt;<sup>ChunkCount</sup>, ChunkCount, DirtyChunkCount, Footer</li>
 * <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 * <li>PackedIntsVersion --&gt; {@link PackedInts#VERSION_CURRENT} as a {@link DataOutput#writeVInt VInt}</li>
 * <li>ChunkCount is not known in advance and is the number of chunks necessary to store all document of the segment</li>
 * <li>Chunk --&gt; DocBase, ChunkDocs, DocFieldCounts, DocLengths, &lt;CompressedDocs&gt;</li>
 * <li>DocBase --&gt; the ID of the first document of the chunk as a {@link DataOutput#writeVInt VInt}</li>
 * <li>ChunkDocs --&gt; the number of documents in the chunk as a {@link DataOutput#writeVInt VInt}</li>
 * <li>DocFieldCounts --&gt; the number of stored fields of every document in the chunk, encoded as followed:<ul>
 *   <li>if chunkDocs=1, the unique value is encoded as a {@link DataOutput#writeVInt VInt}</li>
 *   <li>else read a {@link DataOutput#writeVInt VInt} (let's call it <tt>bitsRequired</tt>)<ul>
 *     <li>if <tt>bitsRequired</tt> is <tt>0</tt> then all values are equal, and the common value is the following {@link DataOutput#writeVInt VInt}</li>
 *     <li>else <tt>bitsRequired</tt> is the number of bits required to store any value, and values are stored in a {@link PackedInts packed} array where every value is stored on exactly <tt>bitsRequired</tt> bits</li>
 *   </ul></li>
 * </ul></li>
 * <li>DocLengths --&gt; the lengths of all documents in the chunk, encoded with the same method as DocFieldCounts</li>
 * <li>CompressedDocs --&gt; a compressed representation of &lt;Docs&gt; using the LZ4 compression format</li>
 * <li>Docs --&gt; &lt;Doc&gt;<sup>ChunkDocs</sup></li>
 * <li>Doc --&gt; &lt;FieldNumAndType, Value&gt;<sup>DocFieldCount</sup></li>
 * <li>FieldNumAndType --&gt; a {@link DataOutput#writeVLong VLong}, whose 3 last bits are Type and other bits are FieldNum</li>
 * <li>Type --&gt;<ul>
 *   <li>0: Value is String</li>
 *   <li>1: Value is BinaryValue</li>
 *   <li>2: Value is Int</li>
 *   <li>3: Value is Float</li>
 *   <li>4: Value is Long</li>
 *   <li>5: Value is Double</li>
 *   <li>6, 7: unused</li>
 * </ul></li>
 * <li>FieldNum --&gt; an ID of the field</li>
 * <li>Value --&gt; {@link DataOutput#writeString(String) String} | BinaryValue | Int | Float | Long | Double depending on Type</li>
 * <li>BinaryValue --&gt; ValueLength &lt;Byte&gt;<sup>ValueLength</sup></li>
 * <li>ChunkCount --&gt; the number of chunks in this file</li>
 * <li>DirtyChunkCount --&gt; the number of prematurely flushed chunks in this file</li>
 * <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes
 * <ul>
 * <li>If documents are larger than 16KB then chunks will likely contain only
 * one document. However, documents can never spread across several chunks (all
 * fields of a single document are in the same chunk).</li>
 * <li>When at least one document in a chunk is large enough so that the chunk
 * is larger than 32KB, the chunk will actually be compressed in several LZ4
 * blocks of 16KB. This allows {@link StoredFieldVisitor}s which are only
 * interested in the first fields of a document to not have to decompress 10MB
 * of data if the document is 10MB, but only 16KB.</li>
 * <li>Given that the original lengths are written in the metadata of the chunk,
 * the decompressor can leverage this information to stop decoding as soon as
 * enough data has been decompressed.</li>
 * <li>In case documents are incompressible, CompressedDocs will be less than
 * 0.5% larger than Docs.</li>
 * </ul>
 * </li>
 * <li><a name="field_index"></a>
 * <p>A fields index file (extension <tt>.fdx</tt>).</p>
 * <ul>
 * <li>FieldsIndex (.fdx) --&gt; &lt;Header&gt;, &lt;ChunkIndex&gt;, Footer</li>
 * <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 * <li>ChunkIndex: See {@link CompressingStoredFieldsIndexWriter}</li>
 * <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * </li>
 * </ol>
 * <p><b>Known limitations</b>
 * <p>This {@link StoredFieldsFormat} does not support individual documents
 * larger than (<tt>2<sup>31</sup> - 2<sup>14</sup></tt>) bytes.
 * @lucene.experimental
 */
public final class Lucene50StoredFieldsFormat extends StoredFieldsFormat {
  
  /** Configuration option for stored fields. */
  public static enum Mode {
    /** Trade compression ratio for retrieval speed. */
    BEST_SPEED,
    /** Trade retrieval speed for compression ratio. */
    BEST_COMPRESSION
  }
  
  /** Attribute key for compression mode. */
  public static final String MODE_KEY = Lucene50StoredFieldsFormat.class.getSimpleName() + ".mode";
  
  final Mode mode;
  
  /** Stored fields format with default options */
  public Lucene50StoredFieldsFormat() {
    this(Mode.BEST_SPEED);
  }
  
  /** Stored fields format with specified mode */
  public Lucene50StoredFieldsFormat(Mode mode) {
    this.mode = Objects.requireNonNull(mode);
  }

  @Override
  public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
    String value = si.getAttribute(MODE_KEY);
    if (value == null) {
      throw new IllegalStateException("missing value for " + MODE_KEY + " for segment: " + si.name);
    }
    Mode mode = Mode.valueOf(value);
    return impl(mode).fieldsReader(directory, si, fn, context);
  }

  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
    String previous = si.putAttribute(MODE_KEY, mode.name());
    if (previous != null) {
      throw new IllegalStateException("found existing value for " + MODE_KEY + " for segment: " + si.name +
                                      "old=" + previous + ", new=" + mode.name());
    }
    return impl(mode).fieldsWriter(directory, si, context);
  }
  
  StoredFieldsFormat impl(Mode mode) {
    switch (mode) {
      case BEST_SPEED: 
        return new CompressingStoredFieldsFormat("Lucene50StoredFieldsFast", CompressionMode.FAST, 1 << 14, 128, 1024);
      case BEST_COMPRESSION: 
        return new CompressingStoredFieldsFormat("Lucene50StoredFieldsHigh", CompressionMode.HIGH_COMPRESSION, 61440, 512, 1024);
      default: throw new AssertionError();
    }
  }
}
