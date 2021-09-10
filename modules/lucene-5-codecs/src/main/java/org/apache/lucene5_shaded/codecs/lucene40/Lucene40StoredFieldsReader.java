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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.StoredFieldsReader;
import org.apache.lucene5_shaded.index.CorruptIndexException;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.index.StoredFieldVisitor;
import org.apache.lucene5_shaded.store.AlreadyClosedException;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.IOUtils;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

/**
 * Reader for 4.0 stored fields
 * @deprecated only for reading 4.0 segments
 */
@Deprecated
final class Lucene40StoredFieldsReader extends StoredFieldsReader implements Cloneable, Closeable {

  // NOTE: bit 0 is free here!  You can steal it!
  static final int FIELD_IS_BINARY = 1 << 1;

  // the old bit 1 << 2 was compressed, is now left out

  private static final int _NUMERIC_BIT_SHIFT = 3;
  static final int FIELD_IS_NUMERIC_MASK = 0x07 << _NUMERIC_BIT_SHIFT;

  static final int FIELD_IS_NUMERIC_INT = 1 << _NUMERIC_BIT_SHIFT;
  static final int FIELD_IS_NUMERIC_LONG = 2 << _NUMERIC_BIT_SHIFT;
  static final int FIELD_IS_NUMERIC_FLOAT = 3 << _NUMERIC_BIT_SHIFT;
  static final int FIELD_IS_NUMERIC_DOUBLE = 4 << _NUMERIC_BIT_SHIFT;

  // the next possible bits are: 1 << 6; 1 << 7
  // currently unused: static final int FIELD_IS_NUMERIC_SHORT = 5 << _NUMERIC_BIT_SHIFT;
  // currently unused: static final int FIELD_IS_NUMERIC_BYTE = 6 << _NUMERIC_BIT_SHIFT;

  static final String CODEC_NAME_IDX = "Lucene40StoredFieldsIndex";
  static final String CODEC_NAME_DAT = "Lucene40StoredFieldsData";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  static final long HEADER_LENGTH_IDX = CodecUtil.headerLength(CODEC_NAME_IDX);
  static final long HEADER_LENGTH_DAT = CodecUtil.headerLength(CODEC_NAME_DAT);



  /** Extension of stored fields file */
  static final String FIELDS_EXTENSION = "fdt";

  /** Extension of stored fields index file */
  static final String FIELDS_INDEX_EXTENSION = "fdx";

  private static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Lucene40StoredFieldsReader.class);

  private final FieldInfos fieldInfos;
  private final IndexInput fieldsStream;
  private final IndexInput indexStream;
  private int numTotalDocs;
  private int size;
  private boolean closed;

  /** Returns a cloned FieldsReader that shares open
   *  IndexInputs with the original one.  It is the caller's
   *  job not to close the original FieldsReader until all
   *  clones are called (eg, currently SegmentReader manages
   *  this logic). */
  @Override
  public Lucene40StoredFieldsReader clone() {
    ensureOpen();
    return new Lucene40StoredFieldsReader(fieldInfos, numTotalDocs, size, fieldsStream.clone(), indexStream.clone());
  }

  /** Used only by clone. */
  private Lucene40StoredFieldsReader(FieldInfos fieldInfos, int numTotalDocs, int size, IndexInput fieldsStream, IndexInput indexStream) {
    this.fieldInfos = fieldInfos;
    this.numTotalDocs = numTotalDocs;
    this.size = size;
    this.fieldsStream = fieldsStream;
    this.indexStream = indexStream;
  }

  /** Sole constructor. */
  public Lucene40StoredFieldsReader(Directory d, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
    final String segment = si.name;
    boolean success = false;
    fieldInfos = fn;
    try {
      fieldsStream = d.openInput(IndexFileNames.segmentFileName(segment, "", FIELDS_EXTENSION), context);
      final String indexStreamFN = IndexFileNames.segmentFileName(segment, "", FIELDS_INDEX_EXTENSION);
      indexStream = d.openInput(indexStreamFN, context);

      CodecUtil.checkHeader(indexStream, CODEC_NAME_IDX, VERSION_START, VERSION_CURRENT);
      CodecUtil.checkHeader(fieldsStream, CODEC_NAME_DAT, VERSION_START, VERSION_CURRENT);
      assert HEADER_LENGTH_DAT == fieldsStream.getFilePointer();
      assert HEADER_LENGTH_IDX == indexStream.getFilePointer();
      final long indexSize = indexStream.length() - HEADER_LENGTH_IDX;
      this.size = (int) (indexSize >> 3);
      // Verify two sources of "maxDoc" agree:
      if (this.size != si.maxDoc()) {
        throw new CorruptIndexException("doc counts differ for segment " + segment + ": fieldsReader shows " + this.size + " but segmentInfo shows " + si.maxDoc(), indexStream);
      }
      numTotalDocs = (int) (indexSize >> 3);
      success = true;
    } finally {
      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above. In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        try {
          close();
        } catch (Throwable t) {} // ensure we throw our original exception
      }
    }
  }

  /**
   * @throws AlreadyClosedException if this FieldsReader is closed
   */
  private void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this FieldsReader is closed");
    }
  }

  /**
   * Closes the underlying {@link IndexInput} streams.
   * This means that the Fields values will not be accessible.
   *
   * @throws IOException If an I/O error occurs
   */
  @Override
  public final void close() throws IOException {
    if (!closed) {
      IOUtils.close(fieldsStream, indexStream);
      closed = true;
    }
  }

  /** Returns number of documents. */
  public final int size() {
    return size;
  }

  private void seekIndex(int docID) throws IOException {
    indexStream.seek(HEADER_LENGTH_IDX + docID * 8L);
  }

  @Override
  public final void visitDocument(int n, StoredFieldVisitor visitor) throws IOException {
    seekIndex(n);
    fieldsStream.seek(indexStream.readLong());

    final int numFields = fieldsStream.readVInt();
    for (int fieldIDX = 0; fieldIDX < numFields; fieldIDX++) {
      int fieldNumber = fieldsStream.readVInt();
      FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);

      int bits = fieldsStream.readByte() & 0xFF;
      assert bits <= (FIELD_IS_NUMERIC_MASK | FIELD_IS_BINARY): "bits=" + Integer.toHexString(bits);

      switch(visitor.needsField(fieldInfo)) {
        case YES:
          readField(visitor, fieldInfo, bits);
          break;
        case NO:
          skipField(bits);
          break;
        case STOP:
          return;
      }
    }
  }

  private void readField(StoredFieldVisitor visitor, FieldInfo info, int bits) throws IOException {
    final int numeric = bits & FIELD_IS_NUMERIC_MASK;
    if (numeric != 0) {
      switch(numeric) {
        case FIELD_IS_NUMERIC_INT:
          visitor.intField(info, fieldsStream.readInt());
          return;
        case FIELD_IS_NUMERIC_LONG:
          visitor.longField(info, fieldsStream.readLong());
          return;
        case FIELD_IS_NUMERIC_FLOAT:
          visitor.floatField(info, Float.intBitsToFloat(fieldsStream.readInt()));
          return;
        case FIELD_IS_NUMERIC_DOUBLE:
          visitor.doubleField(info, Double.longBitsToDouble(fieldsStream.readLong()));
          return;
        default:
          throw new CorruptIndexException("Invalid numeric type: " + Integer.toHexString(numeric), fieldsStream);
      }
    } else {
      final int length = fieldsStream.readVInt();
      byte bytes[] = new byte[length];
      fieldsStream.readBytes(bytes, 0, length);
      if ((bits & FIELD_IS_BINARY) != 0) {
        visitor.binaryField(info, bytes);
      } else {
        visitor.stringField(info, bytes);
      }
    }
  }

  private void skipField(int bits) throws IOException {
    final int numeric = bits & FIELD_IS_NUMERIC_MASK;
    if (numeric != 0) {
      switch(numeric) {
        case FIELD_IS_NUMERIC_INT:
        case FIELD_IS_NUMERIC_FLOAT:
          fieldsStream.readInt();
          return;
        case FIELD_IS_NUMERIC_LONG:
        case FIELD_IS_NUMERIC_DOUBLE:
          fieldsStream.readLong();
          return;
        default:
          throw new CorruptIndexException("Invalid numeric type: " + Integer.toHexString(numeric), fieldsStream);
      }
    } else {
      final int length = fieldsStream.readVInt();
      fieldsStream.seek(fieldsStream.getFilePointer() + length);
    }
  }

  @Override
  public long ramBytesUsed() {
    return RAM_BYTES_USED;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  @Override
  public void checkIntegrity() throws IOException {}

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
