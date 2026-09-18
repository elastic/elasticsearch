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
package org.elasticsearch.index.codec.lucene87;

import org.apache.lucene.backward_codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.elasticsearch.index.codec.lucene50.compressing.Lucene50CompressingStoredFieldsFormat;

import java.io.IOException;
import java.util.Objects;

/**
 * Lucene 8.7 stored fields format.
 *
 * <p><b>Principle</b>
 *
 * <p>This {@link StoredFieldsFormat} compresses blocks of documents in order to improve the
 * compression ratio compared to document-level compression. It uses the <a
 * href="http://code.google.com/p/lz4/">LZ4</a> compression algorithm by default in 16KB blocks,
 * which is fast to compress and very fast to decompress data. Although the default compression
 * method that is used ({@link Mode#BEST_SPEED BEST_SPEED}) focuses more on speed than on
 * compression ratio, it should provide interesting compression ratios for redundant inputs (such as
 * log files, HTML or plain text). For higher compression, you can choose ({@link
 * Mode#BEST_COMPRESSION BEST_COMPRESSION}), which uses the <a
 * href="http://en.wikipedia.org/wiki/DEFLATE">DEFLATE</a> algorithm with 48kB blocks and shared
 * dictionaries for a better ratio at the expense of slower performance. These two options can be
 * configured like this:
 *
 * <pre class="prettyprint">
 *   // the default: for high performance
 *   indexWriterConfig.setCodec(new Lucene87Codec(Mode.BEST_SPEED));
 *   // instead for higher performance (but slower):
 *   // indexWriterConfig.setCodec(new Lucene87Codec(Mode.BEST_COMPRESSION));
 * </pre>
 *
 * <p><b>File formats</b>
 *
 * <p>Stored fields are represented by three files:
 *
 * <ol>
 *   <li><a id="field_data"></a>
 *       <p>A fields data file (extension <code>.fdt</code>). This file stores a compact
 *       representation of documents in compressed blocks of 16KB or more. When writing a segment,
 *       documents are appended to an in-memory <code>byte[]</code> buffer. When its size reaches
 *       16KB or more, some metadata about the documents is flushed to disk, immediately followed by
 *       a compressed representation of the buffer using the <a
 *       href="https://github.com/lz4/lz4">LZ4</a> <a
 *       href="http://fastcompression.blogspot.fr/2011/05/lz4-explained.html">compression
 *       format</a>.
 *       <p>Notes
 *       <ul>
 *         <li>When at least one document in a chunk is large enough so that the chunk is larger
 *             than 32KB, the chunk will actually be compressed in several LZ4 blocks of 16KB. This
 *             allows {@link StoredFieldVisitor}s which are only interested in the first fields of a
 *             document to not have to decompress 10MB of data if the document is 10MB, but only
 *             16KB.
 *         <li>Given that the original lengths are written in the metadata of the chunk, the
 *             decompressor can leverage this information to stop decoding as soon as enough data
 *             has been decompressed.
 *         <li>In case documents are incompressible, the overhead of the compression format is less
 *             than 0.5%.
 *       </ul>
 *   <li><a id="field_index"></a>
 *       <p>A fields index file (extension <code>.fdx</code>). This file stores two {@link
 *       DirectMonotonicWriter monotonic arrays}, one for the first doc IDs of each block of
 *       compressed documents, and another one for the corresponding offsets on disk. At search
 *       time, the array containing doc IDs is binary-searched in order to find the block that
 *       contains the expected doc ID, and the associated offset on disk is retrieved from the
 *       second array.
 *   <li><a id="field_meta"></a>
 *       <p>A fields meta file (extension <code>.fdm</code>). This file stores metadata about the
 *       monotonic arrays stored in the index file.
 * </ol>
 *
 * <p><b>Known limitations</b>
 *
 * <p>This {@link StoredFieldsFormat} does not support individual documents larger than (<code>
 * 2<sup>31</sup> - 2<sup>14</sup></code>) bytes.
 *
 * @lucene.experimental
 */
public class Lucene87StoredFieldsFormat extends StoredFieldsFormat {

    /** Configuration option for stored fields. */
    public enum Mode {
        /** Trade compression ratio for retrieval speed. */
        BEST_SPEED,
        /** Trade retrieval speed for compression ratio. */
        BEST_COMPRESSION
    }

    /** Attribute key for compression mode. */
    public static final String MODE_KEY = Lucene87StoredFieldsFormat.class.getSimpleName() + ".mode";

    final Mode mode;

    /** Stored fields format with default options */
    public Lucene87StoredFieldsFormat() {
        this(Mode.BEST_SPEED);
    }

    /** Stored fields format with specified mode */
    public Lucene87StoredFieldsFormat(Mode mode) {
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
        throw new UnsupportedOperationException("Old codecs may only be used for reading");
    }

    StoredFieldsFormat impl(Mode mode) {
        switch (mode) {
            case BEST_SPEED:
                return new Lucene50CompressingStoredFieldsFormat(
                    "Lucene87StoredFieldsFastData",
                    BEST_SPEED_MODE,
                    BEST_SPEED_BLOCK_LENGTH,
                    1024,
                    10
                );
            case BEST_COMPRESSION:
                return new Lucene50CompressingStoredFieldsFormat(
                    "Lucene87StoredFieldsHighData",
                    BEST_COMPRESSION_MODE,
                    BEST_COMPRESSION_BLOCK_LENGTH,
                    4096,
                    10
                );
            default:
                throw new AssertionError();
        }
    }

    // Shoot for 10 sub blocks of 48kB each.
    /** Block length for {@link Mode#BEST_COMPRESSION} */
    protected static final int BEST_COMPRESSION_BLOCK_LENGTH = 10 * 48 * 1024;

    /** Compression mode for {@link Mode#BEST_COMPRESSION} */
    public static final CompressionMode BEST_COMPRESSION_MODE = new DeflateWithPresetDictCompressionMode();

    // Shoot for 10 sub blocks of 8kB each.
    /** Block length for {@link Mode#BEST_SPEED} */
    protected static final int BEST_SPEED_BLOCK_LENGTH = 10 * 8 * 1024;

    /** Compression mode for {@link Mode#BEST_SPEED} */
    public static final CompressionMode BEST_SPEED_MODE = new LZ4WithPresetDictCompressionMode();
}
