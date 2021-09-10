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
package org.apache.lucene5_shaded.codecs.lucene41;

import java.io.IOException;

import org.apache.lucene5_shaded.codecs.StoredFieldsFormat;
import org.apache.lucene5_shaded.codecs.StoredFieldsReader;
import org.apache.lucene5_shaded.codecs.StoredFieldsWriter;
import org.apache.lucene5_shaded.codecs.compressing.CompressionMode;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;

/**
 * Lucene 4.1 stored fields format.
 * @deprecated only for reading old 4.x segments
 */
@Deprecated
public class Lucene41StoredFieldsFormat extends StoredFieldsFormat {
  static final String FORMAT_NAME = "Lucene41StoredFields";
  static final String SEGMENT_SUFFIX = "";
  static final CompressionMode COMPRESSION_MODE = CompressionMode.FAST;
  static final int CHUNK_SIZE = 1 << 14;

  @Override
  public final StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
    return new Lucene41StoredFieldsReader(directory, si, SEGMENT_SUFFIX, fn, context, FORMAT_NAME, COMPRESSION_MODE);
  }

  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(compressionMode=" + COMPRESSION_MODE + ", chunkSize=" + CHUNK_SIZE + ")";
  }
}
