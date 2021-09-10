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
package org.apache.lucene5_shaded.codecs.lucene42;

import java.io.IOException;

import org.apache.lucene5_shaded.codecs.TermVectorsFormat;
import org.apache.lucene5_shaded.codecs.TermVectorsReader;
import org.apache.lucene5_shaded.codecs.TermVectorsWriter;
import org.apache.lucene5_shaded.codecs.compressing.CompressionMode;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;

/**
 * Lucene 4.2 {@link TermVectorsFormat term vectors format}.
 * @deprecated only for reading old segments
 */
@Deprecated
public class Lucene42TermVectorsFormat extends TermVectorsFormat {
  // this is actually what 4.2 TVF wrote!
  static final String FORMAT_NAME = "Lucene41StoredFields";
  static final String SEGMENT_SUFFIX = "";
  static final CompressionMode COMPRESSION_MODE = CompressionMode.FAST;
  static final int CHUNK_SIZE = 1 << 12;

  @Override
  public final TermVectorsReader vectorsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext context) throws IOException {
    return new Lucene42TermVectorsReader(directory, segmentInfo, SEGMENT_SUFFIX, fieldInfos, context, FORMAT_NAME, COMPRESSION_MODE);
  }

  @Override
  public TermVectorsWriter vectorsWriter(Directory directory, SegmentInfo segmentInfo, IOContext context) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(compressionMode=" + COMPRESSION_MODE + ", chunkSize=" + CHUNK_SIZE + ")";
  }
}
