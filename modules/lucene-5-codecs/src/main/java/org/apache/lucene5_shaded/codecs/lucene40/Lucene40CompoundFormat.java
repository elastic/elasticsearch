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

import org.apache.lucene5_shaded.codecs.CompoundFormat;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;

/**
 * Lucene 4.0 compound file format
 * @deprecated only for reading old 4.x segments
 */
@Deprecated
public final class Lucene40CompoundFormat extends CompoundFormat {
  
  /** Sole constructor. */
  public Lucene40CompoundFormat() {
  }

  @Override
  public Directory getCompoundReader(Directory dir, SegmentInfo si, IOContext context) throws IOException {
    String fileName = IndexFileNames.segmentFileName(si.name, "", COMPOUND_FILE_EXTENSION);
    return new Lucene40CompoundReader(dir, fileName, context, false);
  }

  @Override
  public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
    String fileName = IndexFileNames.segmentFileName(si.name, "", COMPOUND_FILE_EXTENSION);
    try (Directory cfs = new Lucene40CompoundReader(dir, fileName, context, true)) {
      for (String file : si.files()) {
        cfs.copyFrom(dir, file, file, context);
      }
    }
  }
  
  /** Extension of compound file */
  static final String COMPOUND_FILE_EXTENSION = "cfs";
  /** Extension of compound file entries */
  static final String COMPOUND_FILE_ENTRIES_EXTENSION = "cfe";
}
