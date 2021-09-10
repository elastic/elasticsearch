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
package org.apache.lucene5_shaded.codecs.lucene40;

import java.io.IOException;

import org.apache.lucene5_shaded.codecs.TermVectorsFormat;
import org.apache.lucene5_shaded.codecs.TermVectorsReader;
import org.apache.lucene5_shaded.codecs.TermVectorsWriter;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;

/**
 * Lucene 4.0 Term Vectors format.
 * @deprecated only for reading 4.0 and 4.1 segments
 */
@Deprecated
public class Lucene40TermVectorsFormat extends TermVectorsFormat {

  /** Sole constructor. */
  public Lucene40TermVectorsFormat() {
  }
  
  @Override
  public final TermVectorsReader vectorsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext context) throws IOException {
    return new Lucene40TermVectorsReader(directory, segmentInfo, fieldInfos, context);
  }

  @Override
  public TermVectorsWriter vectorsWriter(Directory directory, SegmentInfo segmentInfo, IOContext context) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }
}
