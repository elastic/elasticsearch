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
import java.util.Collections;
import java.util.Map;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.FieldInfosFormat;
import org.apache.lucene5_shaded.codecs.UndeadNormsProducer;
import org.apache.lucene5_shaded.index.CorruptIndexException;
import org.apache.lucene5_shaded.index.DocValuesType;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.IndexOptions;
import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.util.IOUtils;

/**
 * Lucene 4.0 Field Infos format.
 * @deprecated Only for reading old 4.0 and 4.1 segments
 */
@Deprecated
public class Lucene40FieldInfosFormat extends FieldInfosFormat {
  
  /** Sole constructor. */
  public Lucene40FieldInfosFormat() {
  }

  @Override
  public final FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, "", Lucene40FieldInfosFormat.FIELD_INFOS_EXTENSION);
    IndexInput input = directory.openInput(fileName, iocontext);
    
    boolean success = false;
    try {
      CodecUtil.checkHeader(input, Lucene40FieldInfosFormat.CODEC_NAME, 
                                   Lucene40FieldInfosFormat.FORMAT_START, 
                                   Lucene40FieldInfosFormat.FORMAT_CURRENT);

      final int size = input.readVInt(); //read in the size
      FieldInfo infos[] = new FieldInfo[size];

      for (int i = 0; i < size; i++) {
        String name = input.readString();
        final int fieldNumber = input.readVInt();
        if (fieldNumber < 0) {
          throw new CorruptIndexException("invalid field number for field: " + name + ", fieldNumber=" + fieldNumber, input);
        }
        byte bits = input.readByte();
        boolean isIndexed = (bits & Lucene40FieldInfosFormat.IS_INDEXED) != 0;
        boolean storeTermVector = (bits & Lucene40FieldInfosFormat.STORE_TERMVECTOR) != 0;
        boolean omitNorms = (bits & Lucene40FieldInfosFormat.OMIT_NORMS) != 0;
        boolean storePayloads = (bits & Lucene40FieldInfosFormat.STORE_PAYLOADS) != 0;
        final IndexOptions indexOptions;
        if (!isIndexed) {
          indexOptions = IndexOptions.NONE;
        } else if ((bits & Lucene40FieldInfosFormat.OMIT_TERM_FREQ_AND_POSITIONS) != 0) {
          indexOptions = IndexOptions.DOCS;
        } else if ((bits & Lucene40FieldInfosFormat.OMIT_POSITIONS) != 0) {
          indexOptions = IndexOptions.DOCS_AND_FREQS;
        } else if ((bits & Lucene40FieldInfosFormat.STORE_OFFSETS_IN_POSTINGS) != 0) {
          indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        } else {
          indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        }

        // LUCENE-3027: past indices were able to write
        // storePayloads=true when omitTFAP is also true,
        // which is invalid.  We correct that, here:
        if (isIndexed && indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
          storePayloads = false;
        }
        // DV Types are packed in one byte
        byte val = input.readByte();
        final LegacyDocValuesType oldValuesType = getDocValuesType((byte) (val & 0x0F));
        final LegacyDocValuesType oldNormsType = getDocValuesType((byte) ((val >>> 4) & 0x0F));
        final Map<String,String> attributes = input.readStringStringMap();
        if (oldValuesType.mapping != DocValuesType.NONE) {
          attributes.put(LEGACY_DV_TYPE_KEY, oldValuesType.name());
        }
        if (oldNormsType.mapping != DocValuesType.NONE) {
          if (oldNormsType.mapping != DocValuesType.NUMERIC) {
            throw new CorruptIndexException("invalid norm type: " + oldNormsType, input);
          }
          attributes.put(LEGACY_NORM_TYPE_KEY, oldNormsType.name());
        }
        if (isIndexed && omitNorms == false && oldNormsType.mapping == DocValuesType.NONE) {
          // Undead norms!  Lucene40NormsReader will check this and bring norms back from the dead:
          UndeadNormsProducer.setUndead(attributes);
        }
        infos[i] = new FieldInfo(name, fieldNumber, storeTermVector, 
          omitNorms, storePayloads, indexOptions, oldValuesType.mapping, -1, Collections.unmodifiableMap(attributes));
        infos[i].checkConsistency();
      }

      CodecUtil.checkEOF(input);
      FieldInfos fieldInfos = new FieldInfos(infos);
      success = true;
      return fieldInfos;
    } finally {
      if (success) {
        input.close();
      } else {
        IOUtils.closeWhileHandlingException(input);
      }
    }
  }
  
  static final String LEGACY_DV_TYPE_KEY = Lucene40FieldInfosFormat.class.getSimpleName() + ".dvtype";
  static final String LEGACY_NORM_TYPE_KEY = Lucene40FieldInfosFormat.class.getSimpleName() + ".normtype";
  
  // mapping of 4.0 types -> 4.2 types
  static enum LegacyDocValuesType {
    NONE(DocValuesType.NONE),
    VAR_INTS(DocValuesType.NUMERIC),
    FLOAT_32(DocValuesType.NUMERIC),
    FLOAT_64(DocValuesType.NUMERIC),
    BYTES_FIXED_STRAIGHT(DocValuesType.BINARY),
    BYTES_FIXED_DEREF(DocValuesType.BINARY),
    BYTES_VAR_STRAIGHT(DocValuesType.BINARY),
    BYTES_VAR_DEREF(DocValuesType.BINARY),
    FIXED_INTS_16(DocValuesType.NUMERIC),
    FIXED_INTS_32(DocValuesType.NUMERIC),
    FIXED_INTS_64(DocValuesType.NUMERIC),
    FIXED_INTS_8(DocValuesType.NUMERIC),
    BYTES_FIXED_SORTED(DocValuesType.SORTED),
    BYTES_VAR_SORTED(DocValuesType.SORTED);
    
    final DocValuesType mapping;
    LegacyDocValuesType(DocValuesType mapping) {
      this.mapping = mapping;
    }
  }
  
  // decodes a 4.0 type
  private static LegacyDocValuesType getDocValuesType(byte b) {
    return LegacyDocValuesType.values()[b];
  }
  
  @Override
  public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }

  /** Extension of field infos */
  static final String FIELD_INFOS_EXTENSION = "fnm";
  
  static final String CODEC_NAME = "Lucene40FieldInfos";
  static final int FORMAT_START = 0;
  static final int FORMAT_CURRENT = FORMAT_START;
  
  static final byte IS_INDEXED = 0x1;
  static final byte STORE_TERMVECTOR = 0x2;
  static final byte STORE_OFFSETS_IN_POSTINGS = 0x4;
  static final byte OMIT_NORMS = 0x10;
  static final byte STORE_PAYLOADS = 0x20;
  static final byte OMIT_TERM_FREQ_AND_POSITIONS = 0x40;
  static final byte OMIT_POSITIONS = -128;
}
