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

import org.apache.lucene5_shaded.codecs.DocValuesConsumer;
import org.apache.lucene5_shaded.codecs.DocValuesFormat;
import org.apache.lucene5_shaded.codecs.DocValuesProducer;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.SegmentReadState;
import org.apache.lucene5_shaded.index.SegmentWriteState;

/**
 * Lucene 4.0 DocValues format.
 * @deprecated Only for reading old 4.0 and 4.1 segments
 */
@Deprecated
public class Lucene40DocValuesFormat extends DocValuesFormat {
  
  /** Maximum length for each binary doc values field. */
  static final int MAX_BINARY_FIELD_LENGTH = (1 << 15) - 2;
  
  /** Sole constructor. */
  public Lucene40DocValuesFormat() {
    super("Lucene40");
  }
  
  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }
  
  @Override
  public final DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    String filename = IndexFileNames.segmentFileName(state.segmentInfo.name, 
                                                     "dv", 
                                                     Lucene40CompoundFormat.COMPOUND_FILE_EXTENSION);
    return new Lucene40DocValuesReader(state, filename, Lucene40FieldInfosFormat.LEGACY_DV_TYPE_KEY);
  }
  
  // constants for VAR_INTS
  static final String VAR_INTS_CODEC_NAME = "PackedInts";
  static final int VAR_INTS_VERSION_START = 0;
  static final int VAR_INTS_VERSION_CURRENT = VAR_INTS_VERSION_START;
  static final byte VAR_INTS_PACKED = 0x00;
  static final byte VAR_INTS_FIXED_64 = 0x01;
  
  // constants for FIXED_INTS_8, FIXED_INTS_16, FIXED_INTS_32, FIXED_INTS_64
  static final String INTS_CODEC_NAME = "Ints";
  static final int INTS_VERSION_START = 0;
  static final int INTS_VERSION_CURRENT = INTS_VERSION_START;
  
  // constants for FLOAT_32, FLOAT_64
  static final String FLOATS_CODEC_NAME = "Floats";
  static final int FLOATS_VERSION_START = 0;
  static final int FLOATS_VERSION_CURRENT = FLOATS_VERSION_START;
  
  // constants for BYTES_FIXED_STRAIGHT
  static final String BYTES_FIXED_STRAIGHT_CODEC_NAME = "FixedStraightBytes";
  static final int BYTES_FIXED_STRAIGHT_VERSION_START = 0;
  static final int BYTES_FIXED_STRAIGHT_VERSION_CURRENT = BYTES_FIXED_STRAIGHT_VERSION_START;
  
  // constants for BYTES_VAR_STRAIGHT
  static final String BYTES_VAR_STRAIGHT_CODEC_NAME_IDX = "VarStraightBytesIdx";
  static final String BYTES_VAR_STRAIGHT_CODEC_NAME_DAT = "VarStraightBytesDat";
  static final int BYTES_VAR_STRAIGHT_VERSION_START = 0;
  static final int BYTES_VAR_STRAIGHT_VERSION_CURRENT = BYTES_VAR_STRAIGHT_VERSION_START;
  
  // constants for BYTES_FIXED_DEREF
  static final String BYTES_FIXED_DEREF_CODEC_NAME_IDX = "FixedDerefBytesIdx";
  static final String BYTES_FIXED_DEREF_CODEC_NAME_DAT = "FixedDerefBytesDat";
  static final int BYTES_FIXED_DEREF_VERSION_START = 0;
  static final int BYTES_FIXED_DEREF_VERSION_CURRENT = BYTES_FIXED_DEREF_VERSION_START;
  
  // constants for BYTES_VAR_DEREF
  static final String BYTES_VAR_DEREF_CODEC_NAME_IDX = "VarDerefBytesIdx";
  static final String BYTES_VAR_DEREF_CODEC_NAME_DAT = "VarDerefBytesDat";
  static final int BYTES_VAR_DEREF_VERSION_START = 0;
  static final int BYTES_VAR_DEREF_VERSION_CURRENT = BYTES_VAR_DEREF_VERSION_START;
  
  // constants for BYTES_FIXED_SORTED
  static final String BYTES_FIXED_SORTED_CODEC_NAME_IDX = "FixedSortedBytesIdx";
  static final String BYTES_FIXED_SORTED_CODEC_NAME_DAT = "FixedSortedBytesDat";
  static final int BYTES_FIXED_SORTED_VERSION_START = 0;
  static final int BYTES_FIXED_SORTED_VERSION_CURRENT = BYTES_FIXED_SORTED_VERSION_START;
  
  // constants for BYTES_VAR_SORTED
  // NOTE THIS IS NOT A BUG! 4.0 actually screwed this up (VAR_SORTED and VAR_DEREF have same codec header)
  static final String BYTES_VAR_SORTED_CODEC_NAME_IDX = "VarDerefBytesIdx";
  static final String BYTES_VAR_SORTED_CODEC_NAME_DAT = "VarDerefBytesDat";
  static final int BYTES_VAR_SORTED_VERSION_START = 0;
  static final int BYTES_VAR_SORTED_VERSION_CURRENT = BYTES_VAR_SORTED_VERSION_START;
}
