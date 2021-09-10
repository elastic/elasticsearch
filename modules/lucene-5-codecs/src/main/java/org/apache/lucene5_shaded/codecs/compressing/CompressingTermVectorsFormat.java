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
package org.apache.lucene5_shaded.codecs.compressing;


import java.io.IOException;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.StoredFieldsFormat;
import org.apache.lucene5_shaded.codecs.TermVectorsFormat;
import org.apache.lucene5_shaded.codecs.TermVectorsReader;
import org.apache.lucene5_shaded.codecs.TermVectorsWriter;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;

/**
 * A {@link TermVectorsFormat} that compresses chunks of documents together in
 * order to improve the compression ratio.
 * @lucene.experimental
 */
public class CompressingTermVectorsFormat extends TermVectorsFormat {

  private final String formatName;
  private final String segmentSuffix;
  private final CompressionMode compressionMode;
  private final int chunkSize;
  private final int blockSize;

  /**
   * Create a new {@link CompressingTermVectorsFormat}.
   * <p>
   * <code>formatName</code> is the name of the format. This name will be used
   * in the file formats to perform
   * {@link CodecUtil#checkIndexHeader codec header checks}.
   * <p>
   * The <code>compressionMode</code> parameter allows you to choose between
   * compression algorithms that have various compression and decompression
   * speeds so that you can pick the one that best fits your indexing and
   * searching throughput. You should never instantiate two
   * {@link CompressingTermVectorsFormat}s that have the same name but
   * different {@link CompressionMode}s.
   * <p>
   * <code>chunkSize</code> is the minimum byte size of a chunk of documents.
   * Higher values of <code>chunkSize</code> should improve the compression
   * ratio but will require more memory at indexing time and might make document
   * loading a little slower (depending on the size of your OS cache compared
   * to the size of your index).
   *
   * @param formatName the name of the {@link StoredFieldsFormat}
   * @param segmentSuffix a suffix to append to files created by this format
   * @param compressionMode the {@link CompressionMode} to use
   * @param chunkSize the minimum number of bytes of a single chunk of stored documents
   * @param blockSize the number of chunks to store in an index block.
   * @see CompressionMode
   */
  public CompressingTermVectorsFormat(String formatName, String segmentSuffix,
      CompressionMode compressionMode, int chunkSize, int blockSize) {
    this.formatName = formatName;
    this.segmentSuffix = segmentSuffix;
    this.compressionMode = compressionMode;
    if (chunkSize < 1) {
      throw new IllegalArgumentException("chunkSize must be >= 1");
    }
    this.chunkSize = chunkSize;
    if (blockSize < 1) {
      throw new IllegalArgumentException("blockSize must be >= 1");
    }
    this.blockSize = blockSize;
  }

  @Override
  public final TermVectorsReader vectorsReader(Directory directory,
      SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext context)
      throws IOException {
    return new CompressingTermVectorsReader(directory, segmentInfo, segmentSuffix,
        fieldInfos, context, formatName, compressionMode);
  }

  @Override
  public final TermVectorsWriter vectorsWriter(Directory directory,
      SegmentInfo segmentInfo, IOContext context) throws IOException {
    return new CompressingTermVectorsWriter(directory, segmentInfo, segmentSuffix,
        context, formatName, compressionMode, chunkSize, blockSize);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(compressionMode=" + compressionMode
        + ", chunkSize=" + chunkSize + ", blockSize=" + blockSize + ")";
  }

}
