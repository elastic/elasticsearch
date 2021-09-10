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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.TermVectorsReader;
import org.apache.lucene5_shaded.index.CorruptIndexException;
import org.apache.lucene5_shaded.index.DocsAndPositionsEnum;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.Fields;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.BytesRefBuilder;
import org.apache.lucene5_shaded.util.IOUtils;

/**
 * Lucene 4.0 Term Vectors reader.
 * @deprecated only for reading 4.0 and 4.1 segments
 */
@Deprecated
final class Lucene40TermVectorsReader extends TermVectorsReader implements Closeable {

  static final byte STORE_POSITIONS_WITH_TERMVECTOR = 0x1;

  static final byte STORE_OFFSET_WITH_TERMVECTOR = 0x2;
  
  static final byte STORE_PAYLOAD_WITH_TERMVECTOR = 0x4;
  
  /** Extension of vectors fields file */
  static final String VECTORS_FIELDS_EXTENSION = "tvf";

  /** Extension of vectors documents file */
  static final String VECTORS_DOCUMENTS_EXTENSION = "tvd";

  /** Extension of vectors index file */
  static final String VECTORS_INDEX_EXTENSION = "tvx";
  
  static final String CODEC_NAME_FIELDS = "Lucene40TermVectorsFields";
  static final String CODEC_NAME_DOCS = "Lucene40TermVectorsDocs";
  static final String CODEC_NAME_INDEX = "Lucene40TermVectorsIndex";

  static final int VERSION_NO_PAYLOADS = 0;
  static final int VERSION_PAYLOADS = 1;
  static final int VERSION_START = VERSION_NO_PAYLOADS;
  static final int VERSION_CURRENT = VERSION_PAYLOADS;
  
  static final long HEADER_LENGTH_FIELDS = CodecUtil.headerLength(CODEC_NAME_FIELDS);
  static final long HEADER_LENGTH_DOCS = CodecUtil.headerLength(CODEC_NAME_DOCS);
  static final long HEADER_LENGTH_INDEX = CodecUtil.headerLength(CODEC_NAME_INDEX);

  private FieldInfos fieldInfos;

  private IndexInput tvx;
  private IndexInput tvd;
  private IndexInput tvf;
  private int size;
  private int numTotalDocs;
  

  /** Used by clone. */
  Lucene40TermVectorsReader(FieldInfos fieldInfos, IndexInput tvx, IndexInput tvd, IndexInput tvf, int size, int numTotalDocs) {
    this.fieldInfos = fieldInfos;
    this.tvx = tvx;
    this.tvd = tvd;
    this.tvf = tvf;
    this.size = size;
    this.numTotalDocs = numTotalDocs;
  }
    
  /** Sole constructor. */
  public Lucene40TermVectorsReader(Directory d, SegmentInfo si, FieldInfos fieldInfos, IOContext context)
    throws IOException {
    final String segment = si.name;
    final int size = si.maxDoc();
    
    boolean success = false;

    try {
      String idxName = IndexFileNames.segmentFileName(segment, "", VECTORS_INDEX_EXTENSION);
      tvx = d.openInput(idxName, context);
      final int tvxVersion = CodecUtil.checkHeader(tvx, CODEC_NAME_INDEX, VERSION_START, VERSION_CURRENT);
      
      String fn = IndexFileNames.segmentFileName(segment, "", VECTORS_DOCUMENTS_EXTENSION);
      tvd = d.openInput(fn, context);
      final int tvdVersion = CodecUtil.checkHeader(tvd, CODEC_NAME_DOCS, VERSION_START, VERSION_CURRENT);
      fn = IndexFileNames.segmentFileName(segment, "", VECTORS_FIELDS_EXTENSION);
      tvf = d.openInput(fn, context);
      final int tvfVersion = CodecUtil.checkHeader(tvf, CODEC_NAME_FIELDS, VERSION_START, VERSION_CURRENT);
      assert HEADER_LENGTH_INDEX == tvx.getFilePointer();
      assert HEADER_LENGTH_DOCS == tvd.getFilePointer();
      assert HEADER_LENGTH_FIELDS == tvf.getFilePointer();
      if (tvxVersion != tvdVersion) {
        throw new CorruptIndexException("version mismatch: tvx=" + tvxVersion + " != tvd=" + tvdVersion, tvd);
      }
      if (tvxVersion != tvfVersion) {
        throw new CorruptIndexException("version mismatch: tvx=" + tvxVersion + " != tvf=" + tvfVersion, tvf);
      }

      numTotalDocs = (int) (tvx.length()-HEADER_LENGTH_INDEX >> 4);

      this.size = numTotalDocs;
      assert size == 0 || numTotalDocs == size;

      this.fieldInfos = fieldInfos;
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

  // Not private to avoid synthetic access$NNN methods
  void seekTvx(final int docNum) throws IOException {
    tvx.seek(docNum * 16L + HEADER_LENGTH_INDEX);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(tvx, tvd, tvf);
  }

  /**
   * 
   * @return The number of documents in the reader
   */
  int size() {
    return size;
  }

  private class TVFields extends Fields {
    private final int[] fieldNumbers;
    private final long[] fieldFPs;
    private final Map<Integer,Integer> fieldNumberToIndex = new HashMap<>();

    public TVFields(int docID) throws IOException {
      seekTvx(docID);
      tvd.seek(tvx.readLong());
      
      final int fieldCount = tvd.readVInt();
      assert fieldCount >= 0;
      if (fieldCount != 0) {
        fieldNumbers = new int[fieldCount];
        fieldFPs = new long[fieldCount];
        for(int fieldUpto=0;fieldUpto<fieldCount;fieldUpto++) {
          final int fieldNumber = tvd.readVInt();
          fieldNumbers[fieldUpto] = fieldNumber;
          fieldNumberToIndex.put(fieldNumber, fieldUpto);
        }

        long position = tvx.readLong();
        fieldFPs[0] = position;
        for(int fieldUpto=1;fieldUpto<fieldCount;fieldUpto++) {
          position += tvd.readVLong();
          fieldFPs[fieldUpto] = position;
        }
      } else {
        // TODO: we can improve writer here, eg write 0 into
        // tvx file, so we know on first read from tvx that
        // this doc has no TVs
        fieldNumbers = null;
        fieldFPs = null;
      }
    }
    
    @Override
    public Iterator<String> iterator() {
      return new Iterator<String>() {
        private int fieldUpto;

        @Override
        public String next() {
          if (fieldNumbers != null && fieldUpto < fieldNumbers.length) {
            return fieldInfos.fieldInfo(fieldNumbers[fieldUpto++]).name;
          } else {
            throw new NoSuchElementException();
          }
        }

        @Override
        public boolean hasNext() {
          return fieldNumbers != null && fieldUpto < fieldNumbers.length;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public Terms terms(String field) throws IOException {
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      if (fieldInfo == null) {
        // No such field
        return null;
      }

      final Integer fieldIndex = fieldNumberToIndex.get(fieldInfo.number);
      if (fieldIndex == null) {
        // Term vectors were not indexed for this field
        return null;
      }

      return new TVTerms(fieldFPs[fieldIndex]);
    }

    @Override
    public int size() {
      if (fieldNumbers == null) {
        return 0;
      } else {
        return fieldNumbers.length;
      }
    }
  }

  private class TVTerms extends Terms {
    private final int numTerms;
    private final long tvfFPStart;
    private final boolean storePositions;
    private final boolean storeOffsets;
    private final boolean storePayloads;

    public TVTerms(long tvfFP) throws IOException {
      tvf.seek(tvfFP);
      numTerms = tvf.readVInt();
      final byte bits = tvf.readByte();
      storePositions = (bits & STORE_POSITIONS_WITH_TERMVECTOR) != 0;
      storeOffsets = (bits & STORE_OFFSET_WITH_TERMVECTOR) != 0;
      storePayloads = (bits & STORE_PAYLOAD_WITH_TERMVECTOR) != 0;
      tvfFPStart = tvf.getFilePointer();
    }

    @Override
    public TermsEnum iterator() throws IOException {
      TVTermsEnum termsEnum = new TVTermsEnum();
      termsEnum.reset(numTerms, tvfFPStart, storePositions, storeOffsets, storePayloads);
      return termsEnum;
    }

    @Override
    public long size() {
      return numTerms;
    }

    @Override
    public long getSumTotalTermFreq() {
      return -1;
    }

    @Override
    public long getSumDocFreq() {
      // Every term occurs in just one doc:
      return numTerms;
    }

    @Override
    public int getDocCount() {
      return 1;
    }

    @Override
    public boolean hasFreqs() {
      return true;
    }

    @Override
    public boolean hasOffsets() {
      return storeOffsets;
    }

    @Override
    public boolean hasPositions() {
      return storePositions;
    }
    
    @Override
    public boolean hasPayloads() {
      return storePayloads;
    }
  }

  private class TVTermsEnum extends TermsEnum {
    private final IndexInput origTVF;
    private final IndexInput tvf;
    private int numTerms;
    private int nextTerm;
    private int freq;
    private BytesRefBuilder lastTerm = new BytesRefBuilder();
    private BytesRefBuilder term = new BytesRefBuilder();
    private boolean storePositions;
    private boolean storeOffsets;
    private boolean storePayloads;
    private long tvfFP;

    private int[] positions;
    private int[] startOffsets;
    private int[] endOffsets;
    
    // one shared byte[] for any term's payloads
    private int[] payloadOffsets;
    private int lastPayloadLength;
    private byte[] payloadData;

    // NOTE: tvf is pre-positioned by caller
    public TVTermsEnum() {
      this.origTVF = Lucene40TermVectorsReader.this.tvf;
      tvf = origTVF.clone();
    }

    public boolean canReuse(IndexInput tvf) {
      return tvf == origTVF;
    }

    public void reset(int numTerms, long tvfFPStart, boolean storePositions, boolean storeOffsets, boolean storePayloads) throws IOException {
      this.numTerms = numTerms;
      this.storePositions = storePositions;
      this.storeOffsets = storeOffsets;
      this.storePayloads = storePayloads;
      nextTerm = 0;
      tvf.seek(tvfFPStart);
      tvfFP = tvfFPStart;
      positions = null;
      startOffsets = null;
      endOffsets = null;
      payloadOffsets = null;
      payloadData = null;
      lastPayloadLength = -1;
    }

    // NOTE: slow!  (linear scan)
    @Override
    public SeekStatus seekCeil(BytesRef text)
      throws IOException {
      if (nextTerm != 0) {
        final int cmp = text.compareTo(term.get());
        if (cmp < 0) {
          nextTerm = 0;
          tvf.seek(tvfFP);
        } else if (cmp == 0) {
          return SeekStatus.FOUND;
        }
      }

      while (next() != null) {
        final int cmp = text.compareTo(term.get());
        if (cmp < 0) {
          return SeekStatus.NOT_FOUND;
        } else if (cmp == 0) {
          return SeekStatus.FOUND;
        }
      }

      return SeekStatus.END;
    }

    @Override
    public void seekExact(long ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef next() throws IOException {
      if (nextTerm >= numTerms) {
        return null;
      }
      term.copyBytes(lastTerm.get());
      final int start = tvf.readVInt();
      final int deltaLen = tvf.readVInt();
      term.setLength(start + deltaLen);
      term.grow(term.length());
      tvf.readBytes(term.bytes(), start, deltaLen);
      freq = tvf.readVInt();

      if (storePayloads) {
        positions = new int[freq];
        payloadOffsets = new int[freq];
        int totalPayloadLength = 0;
        int pos = 0;
        for(int posUpto=0;posUpto<freq;posUpto++) {
          int code = tvf.readVInt();
          pos += code >>> 1;
          positions[posUpto] = pos;
          if ((code & 1) != 0) {
            // length change
            lastPayloadLength = tvf.readVInt();
          }
          payloadOffsets[posUpto] = totalPayloadLength;
          totalPayloadLength += lastPayloadLength;
          assert totalPayloadLength >= 0;
        }
        payloadData = new byte[totalPayloadLength];
        tvf.readBytes(payloadData, 0, payloadData.length);
      } else if (storePositions /* no payloads */) {
        // TODO: we could maybe reuse last array, if we can
        // somehow be careful about consumer never using two
        // D&PEnums at once...
        positions = new int[freq];
        int pos = 0;
        for(int posUpto=0;posUpto<freq;posUpto++) {
          pos += tvf.readVInt();
          positions[posUpto] = pos;
        }
      }

      if (storeOffsets) {
        startOffsets = new int[freq];
        endOffsets = new int[freq];
        int offset = 0;
        for(int posUpto=0;posUpto<freq;posUpto++) {
          startOffsets[posUpto] = offset + tvf.readVInt();
          offset = endOffsets[posUpto] = startOffsets[posUpto] + tvf.readVInt();
        }
      }

      lastTerm.copyBytes(term.get());
      nextTerm++;
      return term.get();
    }

    @Override
    public BytesRef term() {
      return term.get();
    }

    @Override
    public long ord() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() {
      return 1;
    }

    @Override
    public long totalTermFreq() {
      return freq;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {

      if (PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {
        if (storePositions || storeOffsets) {
          TVPostingsEnum docsAndPositionsEnum;
          if (reuse != null && reuse instanceof TVPostingsEnum) {
            docsAndPositionsEnum = (TVPostingsEnum) reuse;
          } else {
            docsAndPositionsEnum = new TVPostingsEnum();
          }
          docsAndPositionsEnum.reset(positions, startOffsets, endOffsets, payloadOffsets, payloadData);
          return docsAndPositionsEnum;
        } else if (PostingsEnum.featureRequested(flags, DocsAndPositionsEnum.OLD_NULL_SEMANTICS)) {
          return null;
        }
      }

      TVDocsEnum docsEnum;
      if (reuse != null && reuse instanceof TVDocsEnum) {
        docsEnum = (TVDocsEnum) reuse;
      } else {
        docsEnum = new TVDocsEnum();
      }
      docsEnum.reset(freq);
      return docsEnum;
    }

  }

  // NOTE: sort of a silly class, since you can get the
  // freq() already by TermsEnum.totalTermFreq
  private static class TVDocsEnum extends PostingsEnum {
    private boolean didNext;
    private int doc = -1;
    private int freq;

    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      if (!didNext) {
        didNext = true;
        return (doc = 0);
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    public void reset(int freq) {
      this.freq = freq;
      this.doc = -1;
      didNext = false;
    }
    
    @Override
    public long cost() {
      return 1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }
    
    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }
  }

  private static class TVPostingsEnum extends PostingsEnum {
    private boolean didNext;
    private int doc = -1;
    private int nextPos;
    private int[] positions;
    private int[] startOffsets;
    private int[] endOffsets;
    private int[] payloadOffsets;
    private BytesRef payload = new BytesRef();
    private byte[] payloadBytes;

    @Override
    public int freq() throws IOException {
      if (positions != null) {
        return positions.length;
      } else {
        assert startOffsets != null;
        return startOffsets.length;
      }
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      if (!didNext) {
        didNext = true;
        return (doc = 0);
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    public void reset(int[] positions, int[] startOffsets, int[] endOffsets, int[] payloadLengths, byte[] payloadBytes) {
      this.positions = positions;
      this.startOffsets = startOffsets;
      this.endOffsets = endOffsets;
      this.payloadOffsets = payloadLengths;
      this.payloadBytes = payloadBytes;
      this.doc = -1;
      didNext = false;
      nextPos = 0;
    }

    @Override
    public BytesRef getPayload() {
      if (payloadOffsets == null) {
        return null;
      } else {
        int off = payloadOffsets[nextPos-1];
        int end = nextPos == payloadOffsets.length ? payloadBytes.length : payloadOffsets[nextPos];
        if (end - off == 0) {
          return null;
        }
        payload.bytes = payloadBytes;
        payload.offset = off;
        payload.length = end - off;
        return payload;
      }
    }

    @Override
    public int nextPosition() {
      assert (positions != null && nextPos < positions.length) ||
        startOffsets != null && nextPos < startOffsets.length;

      if (positions != null) {
        return positions[nextPos++];
      } else {
        nextPos++;
        return -1;
      }
    }

    @Override
    public int startOffset() {
      if (startOffsets == null) {
        return -1;
      } else {
        return startOffsets[nextPos-1];
      }
    }

    @Override
    public int endOffset() {
      if (endOffsets == null) {
        return -1;
      } else {
        return endOffsets[nextPos-1];
      }
    }
    
    @Override
    public long cost() {
      return 1;
    }
  }

  @Override
  public Fields get(int docID) throws IOException {
    if (tvx != null) {
      Fields fields = new TVFields(docID);
      if (fields.size() == 0) {
        // TODO: we can improve writer here, eg write 0 into
        // tvx file, so we know on first read from tvx that
        // this doc has no TVs
        return null;
      } else {
        return fields;
      }
    } else {
      return null;
    }
  }

  @Override
  public TermVectorsReader clone() {
    IndexInput cloneTvx = null;
    IndexInput cloneTvd = null;
    IndexInput cloneTvf = null;

    // These are null when a TermVectorsReader was created
    // on a segment that did not have term vectors saved
    if (tvx != null && tvd != null && tvf != null) {
      cloneTvx = tvx.clone();
      cloneTvd = tvd.clone();
      cloneTvf = tvf.clone();
    }
    
    return new Lucene40TermVectorsReader(fieldInfos, cloneTvx, cloneTvd, cloneTvf, size, numTotalDocs);
  }

  @Override
  public long ramBytesUsed() {
    return 0;
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

