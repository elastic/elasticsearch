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


import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.TermVectorsReader;
import org.apache.lucene5_shaded.index.CorruptIndexException;
import org.apache.lucene5_shaded.index.DocsAndPositionsEnum;
import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.Fields;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.store.AlreadyClosedException;
import org.apache.lucene5_shaded.store.ByteArrayDataInput;
import org.apache.lucene5_shaded.store.ChecksumIndexInput;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Accountables;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.IOUtils;
import org.apache.lucene5_shaded.util.LongsRef;
import org.apache.lucene5_shaded.util.packed.BlockPackedReaderIterator;
import org.apache.lucene5_shaded.util.packed.PackedInts;

import static org.apache.lucene5_shaded.codecs.compressing.CompressingTermVectorsWriter.CODEC_SFX_DAT;
import static org.apache.lucene5_shaded.codecs.compressing.CompressingTermVectorsWriter.CODEC_SFX_IDX;
import static org.apache.lucene5_shaded.codecs.compressing.CompressingTermVectorsWriter.FLAGS_BITS;
import static org.apache.lucene5_shaded.codecs.compressing.CompressingTermVectorsWriter.OFFSETS;
import static org.apache.lucene5_shaded.codecs.compressing.CompressingTermVectorsWriter.PACKED_BLOCK_SIZE;
import static org.apache.lucene5_shaded.codecs.compressing.CompressingTermVectorsWriter.PAYLOADS;
import static org.apache.lucene5_shaded.codecs.compressing.CompressingTermVectorsWriter.POSITIONS;
import static org.apache.lucene5_shaded.codecs.compressing.CompressingTermVectorsWriter.VECTORS_EXTENSION;
import static org.apache.lucene5_shaded.codecs.compressing.CompressingTermVectorsWriter.VECTORS_INDEX_EXTENSION;
import static org.apache.lucene5_shaded.codecs.compressing.CompressingTermVectorsWriter.VERSION_CHUNK_STATS;
import static org.apache.lucene5_shaded.codecs.compressing.CompressingTermVectorsWriter.VERSION_CURRENT;
import static org.apache.lucene5_shaded.codecs.compressing.CompressingTermVectorsWriter.VERSION_START;

/**
 * {@link TermVectorsReader} for {@link CompressingTermVectorsFormat}.
 * @lucene.experimental
 */
public final class CompressingTermVectorsReader extends TermVectorsReader implements Closeable {

  private final FieldInfos fieldInfos;
  final CompressingStoredFieldsIndexReader indexReader;
  final IndexInput vectorsStream;
  private final int version;
  private final int packedIntsVersion;
  private final CompressionMode compressionMode;
  private final Decompressor decompressor;
  private final int chunkSize;
  private final int numDocs;
  private boolean closed;
  private final BlockPackedReaderIterator reader;
  private final long numChunks; // number of compressed blocks written
  private final long numDirtyChunks; // number of incomplete compressed blocks written
  private final long maxPointer; // end of the data section

  // used by clone
  private CompressingTermVectorsReader(CompressingTermVectorsReader reader) {
    this.fieldInfos = reader.fieldInfos;
    this.vectorsStream = reader.vectorsStream.clone();
    this.indexReader = reader.indexReader.clone();
    this.packedIntsVersion = reader.packedIntsVersion;
    this.compressionMode = reader.compressionMode;
    this.decompressor = reader.decompressor.clone();
    this.chunkSize = reader.chunkSize;
    this.numDocs = reader.numDocs;
    this.reader = new BlockPackedReaderIterator(vectorsStream, packedIntsVersion, PACKED_BLOCK_SIZE, 0);
    this.version = reader.version;
    this.numChunks = reader.numChunks;
    this.numDirtyChunks = reader.numDirtyChunks;
    this.maxPointer = reader.maxPointer;
    this.closed = false;
  }

  /** Sole constructor. */
  public CompressingTermVectorsReader(Directory d, SegmentInfo si, String segmentSuffix, FieldInfos fn,
      IOContext context, String formatName, CompressionMode compressionMode) throws IOException {
    this.compressionMode = compressionMode;
    final String segment = si.name;
    boolean success = false;
    fieldInfos = fn;
    numDocs = si.maxDoc();
    int version = -1;
    CompressingStoredFieldsIndexReader indexReader = null;
    
    long maxPointer = -1;
    
    // Load the index into memory
    final String indexName = IndexFileNames.segmentFileName(segment, segmentSuffix, VECTORS_INDEX_EXTENSION);
    try (ChecksumIndexInput input = d.openChecksumInput(indexName, context)) {
      Throwable priorE = null;
      try {
        final String codecNameIdx = formatName + CODEC_SFX_IDX;
        version = CodecUtil.checkIndexHeader(input, codecNameIdx, VERSION_START, VERSION_CURRENT, si.getId(), segmentSuffix);
        assert CodecUtil.indexHeaderLength(codecNameIdx, segmentSuffix) == input.getFilePointer();
        indexReader = new CompressingStoredFieldsIndexReader(input, si);
        maxPointer = input.readVLong(); // the end of the data section
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(input, priorE);
      }
    }
    
    this.version = version;
    this.indexReader = indexReader;
    this.maxPointer = maxPointer;

    try {
      // Open the data file and read metadata
      final String vectorsStreamFN = IndexFileNames.segmentFileName(segment, segmentSuffix, VECTORS_EXTENSION);
      vectorsStream = d.openInput(vectorsStreamFN, context);
      final String codecNameDat = formatName + CODEC_SFX_DAT;
      int version2 = CodecUtil.checkIndexHeader(vectorsStream, codecNameDat, VERSION_START, VERSION_CURRENT, si.getId(), segmentSuffix);
      if (version != version2) {
        throw new CorruptIndexException("Version mismatch between stored fields index and data: " + version + " != " + version2, vectorsStream);
      }
      assert CodecUtil.indexHeaderLength(codecNameDat, segmentSuffix) == vectorsStream.getFilePointer();
      
      long pos = vectorsStream.getFilePointer();
      
      if (version >= VERSION_CHUNK_STATS) {
        vectorsStream.seek(maxPointer);
        numChunks = vectorsStream.readVLong();
        numDirtyChunks = vectorsStream.readVLong();
        if (numDirtyChunks > numChunks) {
          throw new CorruptIndexException("invalid chunk counts: dirty=" + numDirtyChunks + ", total=" + numChunks, vectorsStream);
        }
      } else {
        numChunks = numDirtyChunks = -1;
      }
      
      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(vectorsStream);
      vectorsStream.seek(pos);

      packedIntsVersion = vectorsStream.readVInt();
      chunkSize = vectorsStream.readVInt();
      decompressor = compressionMode.newDecompressor();
      this.reader = new BlockPackedReaderIterator(vectorsStream, packedIntsVersion, PACKED_BLOCK_SIZE, 0);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  CompressionMode getCompressionMode() {
    return compressionMode;
  }

  int getChunkSize() {
    return chunkSize;
  }

  int getPackedIntsVersion() {
    return packedIntsVersion;
  }
  
  int getVersion() {
    return version;
  }

  CompressingStoredFieldsIndexReader getIndexReader() {
    return indexReader;
  }

  IndexInput getVectorsStream() {
    return vectorsStream;
  }
  
  long getMaxPointer() {
    return maxPointer;
  }
  
  long getNumChunks() {
    return numChunks;
  }
  
  long getNumDirtyChunks() {
    return numDirtyChunks;
  }

  /**
   * @throws AlreadyClosedException if this TermVectorsReader is closed
   */
  private void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this FieldsReader is closed");
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      IOUtils.close(vectorsStream);
      closed = true;
    }
  }

  @Override
  public TermVectorsReader clone() {
    return new CompressingTermVectorsReader(this);
  }

  @Override
  public Fields get(int doc) throws IOException {
    ensureOpen();

    // seek to the right place
    {
      final long startPointer = indexReader.getStartPointer(doc);
      vectorsStream.seek(startPointer);
    }

    // decode
    // - docBase: first doc ID of the chunk
    // - chunkDocs: number of docs of the chunk
    final int docBase = vectorsStream.readVInt();
    final int chunkDocs = vectorsStream.readVInt();
    if (doc < docBase || doc >= docBase + chunkDocs || docBase + chunkDocs > numDocs) {
      throw new CorruptIndexException("docBase=" + docBase + ",chunkDocs=" + chunkDocs + ",doc=" + doc, vectorsStream);
    }

    final int skip; // number of fields to skip
    final int numFields; // number of fields of the document we're looking for
    final int totalFields; // total number of fields of the chunk (sum for all docs)
    if (chunkDocs == 1) {
      skip = 0;
      numFields = totalFields = vectorsStream.readVInt();
    } else {
      reader.reset(vectorsStream, chunkDocs);
      int sum = 0;
      for (int i = docBase; i < doc; ++i) {
        sum += reader.next();
      }
      skip = sum;
      numFields = (int) reader.next();
      sum += numFields;
      for (int i = doc + 1; i < docBase + chunkDocs; ++i) {
        sum += reader.next();
      }
      totalFields = sum;
    }

    if (numFields == 0) {
      // no vectors
      return null;
    }

    // read field numbers that have term vectors
    final int[] fieldNums;
    {
      final int token = vectorsStream.readByte() & 0xFF;
      assert token != 0; // means no term vectors, cannot happen since we checked for numFields == 0
      final int bitsPerFieldNum = token & 0x1F;
      int totalDistinctFields = token >>> 5;
      if (totalDistinctFields == 0x07) {
        totalDistinctFields += vectorsStream.readVInt();
      }
      ++totalDistinctFields;
      final PackedInts.ReaderIterator it = PackedInts.getReaderIteratorNoHeader(vectorsStream, PackedInts.Format.PACKED, packedIntsVersion, totalDistinctFields, bitsPerFieldNum, 1);
      fieldNums = new int[totalDistinctFields];
      for (int i = 0; i < totalDistinctFields; ++i) {
        fieldNums[i] = (int) it.next();
      }
    }

    // read field numbers and flags
    final int[] fieldNumOffs = new int[numFields];
    final PackedInts.Reader flags;
    {
      final int bitsPerOff = PackedInts.bitsRequired(fieldNums.length - 1);
      final PackedInts.Reader allFieldNumOffs = PackedInts.getReaderNoHeader(vectorsStream, PackedInts.Format.PACKED, packedIntsVersion, totalFields, bitsPerOff);
      switch (vectorsStream.readVInt()) {
        case 0:
          final PackedInts.Reader fieldFlags = PackedInts.getReaderNoHeader(vectorsStream, PackedInts.Format.PACKED, packedIntsVersion, fieldNums.length, FLAGS_BITS);
          PackedInts.Mutable f = PackedInts.getMutable(totalFields, FLAGS_BITS, PackedInts.COMPACT);
          for (int i = 0; i < totalFields; ++i) {
            final int fieldNumOff = (int) allFieldNumOffs.get(i);
            assert fieldNumOff >= 0 && fieldNumOff < fieldNums.length;
            final int fgs = (int) fieldFlags.get(fieldNumOff);
            f.set(i, fgs);
          }
          flags = f;
          break;
        case 1:
          flags = PackedInts.getReaderNoHeader(vectorsStream, PackedInts.Format.PACKED, packedIntsVersion, totalFields, FLAGS_BITS);
          break;
        default:
          throw new AssertionError();
      }
      for (int i = 0; i < numFields; ++i) {
        fieldNumOffs[i] = (int) allFieldNumOffs.get(skip + i);
      }
    }

    // number of terms per field for all fields
    final PackedInts.Reader numTerms;
    final int totalTerms;
    {
      final int bitsRequired = vectorsStream.readVInt();
      numTerms = PackedInts.getReaderNoHeader(vectorsStream, PackedInts.Format.PACKED, packedIntsVersion, totalFields, bitsRequired);
      int sum = 0;
      for (int i = 0; i < totalFields; ++i) {
        sum += numTerms.get(i);
      }
      totalTerms = sum;
    }

    // term lengths
    int docOff = 0, docLen = 0, totalLen;
    final int[] fieldLengths = new int[numFields];
    final int[][] prefixLengths = new int[numFields][];
    final int[][] suffixLengths = new int[numFields][];
    {
      reader.reset(vectorsStream, totalTerms);
      // skip
      int toSkip = 0;
      for (int i = 0; i < skip; ++i) {
        toSkip += numTerms.get(i);
      }
      reader.skip(toSkip);
      // read prefix lengths
      for (int i = 0; i < numFields; ++i) {
        final int termCount = (int) numTerms.get(skip + i);
        final int[] fieldPrefixLengths = new int[termCount];
        prefixLengths[i] = fieldPrefixLengths;
        for (int j = 0; j < termCount; ) {
          final LongsRef next = reader.next(termCount - j);
          for (int k = 0; k < next.length; ++k) {
            fieldPrefixLengths[j++] = (int) next.longs[next.offset + k];
          }
        }
      }
      reader.skip(totalTerms - reader.ord());

      reader.reset(vectorsStream, totalTerms);
      // skip
      toSkip = 0;
      for (int i = 0; i < skip; ++i) {
        for (int j = 0; j < numTerms.get(i); ++j) {
          docOff += reader.next();
        }
      }
      for (int i = 0; i < numFields; ++i) {
        final int termCount = (int) numTerms.get(skip + i);
        final int[] fieldSuffixLengths = new int[termCount];
        suffixLengths[i] = fieldSuffixLengths;
        for (int j = 0; j < termCount; ) {
          final LongsRef next = reader.next(termCount - j);
          for (int k = 0; k < next.length; ++k) {
            fieldSuffixLengths[j++] = (int) next.longs[next.offset + k];
          }
        }
        fieldLengths[i] = sum(suffixLengths[i]);
        docLen += fieldLengths[i];
      }
      totalLen = docOff + docLen;
      for (int i = skip + numFields; i < totalFields; ++i) {
        for (int j = 0; j < numTerms.get(i); ++j) {
          totalLen += reader.next();
        }
      }
    }

    // term freqs
    final int[] termFreqs = new int[totalTerms];
    {
      reader.reset(vectorsStream, totalTerms);
      for (int i = 0; i < totalTerms; ) {
        final LongsRef next = reader.next(totalTerms - i);
        for (int k = 0; k < next.length; ++k) {
          termFreqs[i++] = 1 + (int) next.longs[next.offset + k];
        }
      }
    }

    // total number of positions, offsets and payloads
    int totalPositions = 0, totalOffsets = 0, totalPayloads = 0;
    for (int i = 0, termIndex = 0; i < totalFields; ++i) {
      final int f = (int) flags.get(i);
      final int termCount = (int) numTerms.get(i);
      for (int j = 0; j < termCount; ++j) {
        final int freq = termFreqs[termIndex++];
        if ((f & POSITIONS) != 0) {
          totalPositions += freq;
        }
        if ((f & OFFSETS) != 0) {
          totalOffsets += freq;
        }
        if ((f & PAYLOADS) != 0) {
          totalPayloads += freq;
        }
      }
      assert i != totalFields - 1 || termIndex == totalTerms : termIndex + " " + totalTerms;
    }

    final int[][] positionIndex = positionIndex(skip, numFields, numTerms, termFreqs);
    final int[][] positions, startOffsets, lengths;
    if (totalPositions > 0) {
      positions = readPositions(skip, numFields, flags, numTerms, termFreqs, POSITIONS, totalPositions, positionIndex);
    } else {
      positions = new int[numFields][];
    }

    if (totalOffsets > 0) {
      // average number of chars per term
      final float[] charsPerTerm = new float[fieldNums.length];
      for (int i = 0; i < charsPerTerm.length; ++i) {
        charsPerTerm[i] = Float.intBitsToFloat(vectorsStream.readInt());
      }
      startOffsets = readPositions(skip, numFields, flags, numTerms, termFreqs, OFFSETS, totalOffsets, positionIndex);
      lengths = readPositions(skip, numFields, flags, numTerms, termFreqs, OFFSETS, totalOffsets, positionIndex);

      for (int i = 0; i < numFields; ++i) {
        final int[] fStartOffsets = startOffsets[i];
        final int[] fPositions = positions[i];
        // patch offsets from positions
        if (fStartOffsets != null && fPositions != null) {
          final float fieldCharsPerTerm = charsPerTerm[fieldNumOffs[i]];
          for (int j = 0; j < startOffsets[i].length; ++j) {
            fStartOffsets[j] += (int) (fieldCharsPerTerm * fPositions[j]);
          }
        }
        if (fStartOffsets != null) {
          final int[] fPrefixLengths = prefixLengths[i];
          final int[] fSuffixLengths = suffixLengths[i];
          final int[] fLengths = lengths[i];
          for (int j = 0, end = (int) numTerms.get(skip + i); j < end; ++j) {
            // delta-decode start offsets and  patch lengths using term lengths
            final int termLength = fPrefixLengths[j] + fSuffixLengths[j];
            lengths[i][positionIndex[i][j]] += termLength;
            for (int k = positionIndex[i][j] + 1; k < positionIndex[i][j + 1]; ++k) {
              fStartOffsets[k] += fStartOffsets[k - 1];
              fLengths[k] += termLength;
            }
          }
        }
      }
    } else {
      startOffsets = lengths = new int[numFields][];
    }
    if (totalPositions > 0) {
      // delta-decode positions
      for (int i = 0; i < numFields; ++i) {
        final int[] fPositions = positions[i];
        final int[] fpositionIndex = positionIndex[i];
        if (fPositions != null) {
          for (int j = 0, end = (int) numTerms.get(skip + i); j < end; ++j) {
            // delta-decode start offsets
            for (int k = fpositionIndex[j] + 1; k < fpositionIndex[j + 1]; ++k) {
              fPositions[k] += fPositions[k - 1];
            }
          }
        }
      }
    }

    // payload lengths
    final int[][] payloadIndex = new int[numFields][];
    int totalPayloadLength = 0;
    int payloadOff = 0;
    int payloadLen = 0;
    if (totalPayloads > 0) {
      reader.reset(vectorsStream, totalPayloads);
      // skip
      int termIndex = 0;
      for (int i = 0; i < skip; ++i) {
        final int f = (int) flags.get(i);
        final int termCount = (int) numTerms.get(i);
        if ((f & PAYLOADS) != 0) {
          for (int j = 0; j < termCount; ++j) {
            final int freq = termFreqs[termIndex + j];
            for (int k = 0; k < freq; ++k) {
              final int l = (int) reader.next();
              payloadOff += l;
            }
          }
        }
        termIndex += termCount;
      }
      totalPayloadLength = payloadOff;
      // read doc payload lengths
      for (int i = 0; i < numFields; ++i) {
        final int f = (int) flags.get(skip + i);
        final int termCount = (int) numTerms.get(skip + i);
        if ((f & PAYLOADS) != 0) {
          final int totalFreq = positionIndex[i][termCount];
          payloadIndex[i] = new int[totalFreq + 1];
          int posIdx = 0;
          payloadIndex[i][posIdx] = payloadLen;
          for (int j = 0; j < termCount; ++j) {
            final int freq = termFreqs[termIndex + j];
            for (int k = 0; k < freq; ++k) {
              final int payloadLength = (int) reader.next();
              payloadLen += payloadLength;
              payloadIndex[i][posIdx+1] = payloadLen;
              ++posIdx;
            }
          }
          assert posIdx == totalFreq;
        }
        termIndex += termCount;
      }
      totalPayloadLength += payloadLen;
      for (int i = skip + numFields; i < totalFields; ++i) {
        final int f = (int) flags.get(i);
        final int termCount = (int) numTerms.get(i);
        if ((f & PAYLOADS) != 0) {
          for (int j = 0; j < termCount; ++j) {
            final int freq = termFreqs[termIndex + j];
            for (int k = 0; k < freq; ++k) {
              totalPayloadLength += reader.next();
            }
          }
        }
        termIndex += termCount;
      }
      assert termIndex == totalTerms : termIndex + " " + totalTerms;
    }

    // decompress data
    final BytesRef suffixBytes = new BytesRef();
    decompressor.decompress(vectorsStream, totalLen + totalPayloadLength, docOff + payloadOff, docLen + payloadLen, suffixBytes);
    suffixBytes.length = docLen;
    final BytesRef payloadBytes = new BytesRef(suffixBytes.bytes, suffixBytes.offset + docLen, payloadLen);

    final int[] fieldFlags = new int[numFields];
    for (int i = 0; i < numFields; ++i) {
      fieldFlags[i] = (int) flags.get(skip + i);
    }

    final int[] fieldNumTerms = new int[numFields];
    for (int i = 0; i < numFields; ++i) {
      fieldNumTerms[i] = (int) numTerms.get(skip + i);
    }

    final int[][] fieldTermFreqs = new int[numFields][];
    {
      int termIdx = 0;
      for (int i = 0; i < skip; ++i) {
        termIdx += numTerms.get(i);
      }
      for (int i = 0; i < numFields; ++i) {
        final int termCount = (int) numTerms.get(skip + i);
        fieldTermFreqs[i] = new int[termCount];
        for (int j = 0; j < termCount; ++j) {
          fieldTermFreqs[i][j] = termFreqs[termIdx++];
        }
      }
    }

    assert sum(fieldLengths) == docLen : sum(fieldLengths) + " != " + docLen;

    return new TVFields(fieldNums, fieldFlags, fieldNumOffs, fieldNumTerms, fieldLengths,
        prefixLengths, suffixLengths, fieldTermFreqs,
        positionIndex, positions, startOffsets, lengths,
        payloadBytes, payloadIndex,
        suffixBytes);
  }

  // field -> term index -> position index
  private int[][] positionIndex(int skip, int numFields, PackedInts.Reader numTerms, int[] termFreqs) {
    final int[][] positionIndex = new int[numFields][];
    int termIndex = 0;
    for (int i = 0; i < skip; ++i) {
      final int termCount = (int) numTerms.get(i);
      termIndex += termCount;
    }
    for (int i = 0; i < numFields; ++i) {
      final int termCount = (int) numTerms.get(skip + i);
      positionIndex[i] = new int[termCount + 1];
      for (int j = 0; j < termCount; ++j) {
        final int freq = termFreqs[termIndex+j];
        positionIndex[i][j + 1] = positionIndex[i][j] + freq;
      }
      termIndex += termCount;
    }
    return positionIndex;
  }

  private int[][] readPositions(int skip, int numFields, PackedInts.Reader flags, PackedInts.Reader numTerms, int[] termFreqs, int flag, final int totalPositions, int[][] positionIndex) throws IOException {
    final int[][] positions = new int[numFields][];
    reader.reset(vectorsStream, totalPositions);
    // skip
    int toSkip = 0;
    int termIndex = 0;
    for (int i = 0; i < skip; ++i) {
      final int f = (int) flags.get(i);
      final int termCount = (int) numTerms.get(i);
      if ((f & flag) != 0) {
        for (int j = 0; j < termCount; ++j) {
          final int freq = termFreqs[termIndex+j];
          toSkip += freq;
        }
      }
      termIndex += termCount;
    }
    reader.skip(toSkip);
    // read doc positions
    for (int i = 0; i < numFields; ++i) {
      final int f = (int) flags.get(skip + i);
      final int termCount = (int) numTerms.get(skip + i);
      if ((f & flag) != 0) {
        final int totalFreq = positionIndex[i][termCount];
        final int[] fieldPositions = new int[totalFreq];
        positions[i] = fieldPositions;
        for (int j = 0; j < totalFreq; ) {
          final LongsRef nextPositions = reader.next(totalFreq - j);
          for (int k = 0; k < nextPositions.length; ++k) {
            fieldPositions[j++] = (int) nextPositions.longs[nextPositions.offset + k];
          }
        }
      }
      termIndex += termCount;
    }
    reader.skip(totalPositions - reader.ord());
    return positions;
  }

  private class TVFields extends Fields {

    private final int[] fieldNums, fieldFlags, fieldNumOffs, numTerms, fieldLengths;
    private final int[][] prefixLengths, suffixLengths, termFreqs, positionIndex, positions, startOffsets, lengths, payloadIndex;
    private final BytesRef suffixBytes, payloadBytes;

    public TVFields(int[] fieldNums, int[] fieldFlags, int[] fieldNumOffs, int[] numTerms, int[] fieldLengths,
        int[][] prefixLengths, int[][] suffixLengths, int[][] termFreqs,
        int[][] positionIndex, int[][] positions, int[][] startOffsets, int[][] lengths,
        BytesRef payloadBytes, int[][] payloadIndex,
        BytesRef suffixBytes) {
      this.fieldNums = fieldNums;
      this.fieldFlags = fieldFlags;
      this.fieldNumOffs = fieldNumOffs;
      this.numTerms = numTerms;
      this.fieldLengths = fieldLengths;
      this.prefixLengths = prefixLengths;
      this.suffixLengths = suffixLengths;
      this.termFreqs = termFreqs;
      this.positionIndex = positionIndex;
      this.positions = positions;
      this.startOffsets = startOffsets;
      this.lengths = lengths;
      this.payloadBytes = payloadBytes;
      this.payloadIndex = payloadIndex;
      this.suffixBytes = suffixBytes;
    }

    @Override
    public Iterator<String> iterator() {
      return new Iterator<String>() {
        int i = 0;
        @Override
        public boolean hasNext() {
          return i < fieldNumOffs.length;
        }
        @Override
        public String next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          final int fieldNum = fieldNums[fieldNumOffs[i++]];
          return fieldInfos.fieldInfo(fieldNum).name;
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
        return null;
      }
      int idx = -1;
      for (int i = 0; i < fieldNumOffs.length; ++i) {
        if (fieldNums[fieldNumOffs[i]] == fieldInfo.number) {
          idx = i;
          break;
        }
      }

      if (idx == -1 || numTerms[idx] == 0) {
        // no term
        return null;
      }
      int fieldOff = 0, fieldLen = -1;
      for (int i = 0; i < fieldNumOffs.length; ++i) {
        if (i < idx) {
          fieldOff += fieldLengths[i];
        } else {
          fieldLen = fieldLengths[i];
          break;
        }
      }
      assert fieldLen >= 0;
      return new TVTerms(numTerms[idx], fieldFlags[idx],
          prefixLengths[idx], suffixLengths[idx], termFreqs[idx],
          positionIndex[idx], positions[idx], startOffsets[idx], lengths[idx],
          payloadIndex[idx], payloadBytes,
          new BytesRef(suffixBytes.bytes, suffixBytes.offset + fieldOff, fieldLen));
    }

    @Override
    public int size() {
      return fieldNumOffs.length;
    }

  }

  private class TVTerms extends Terms {

    private final int numTerms, flags;
    private final int[] prefixLengths, suffixLengths, termFreqs, positionIndex, positions, startOffsets, lengths, payloadIndex;
    private final BytesRef termBytes, payloadBytes;

    TVTerms(int numTerms, int flags, int[] prefixLengths, int[] suffixLengths, int[] termFreqs,
        int[] positionIndex, int[] positions, int[] startOffsets, int[] lengths,
        int[] payloadIndex, BytesRef payloadBytes,
        BytesRef termBytes) {
      this.numTerms = numTerms;
      this.flags = flags;
      this.prefixLengths = prefixLengths;
      this.suffixLengths = suffixLengths;
      this.termFreqs = termFreqs;
      this.positionIndex = positionIndex;
      this.positions = positions;
      this.startOffsets = startOffsets;
      this.lengths = lengths;
      this.payloadIndex = payloadIndex;
      this.payloadBytes = payloadBytes;
      this.termBytes = termBytes;
    }

    @Override
    public TermsEnum iterator() throws IOException {
      TVTermsEnum termsEnum = new TVTermsEnum();
      termsEnum.reset(numTerms, flags, prefixLengths, suffixLengths, termFreqs, positionIndex, positions, startOffsets, lengths,
          payloadIndex, payloadBytes,
          new ByteArrayDataInput(termBytes.bytes, termBytes.offset, termBytes.length));
      return termsEnum;
    }

    @Override
    public long size() throws IOException {
      return numTerms;
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
      return -1L;
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return numTerms;
    }

    @Override
    public int getDocCount() throws IOException {
      return 1;
    }

    @Override
    public boolean hasFreqs() {
      return true;
    }

    @Override
    public boolean hasOffsets() {
      return (flags & OFFSETS) != 0;
    }

    @Override
    public boolean hasPositions() {
      return (flags & POSITIONS) != 0;
    }

    @Override
    public boolean hasPayloads() {
      return (flags & PAYLOADS) != 0;
    }

  }

  private static class TVTermsEnum extends TermsEnum {

    private int numTerms, startPos, ord;
    private int[] prefixLengths, suffixLengths, termFreqs, positionIndex, positions, startOffsets, lengths, payloadIndex;
    private ByteArrayDataInput in;
    private BytesRef payloads;
    private final BytesRef term;

    private TVTermsEnum() {
      term = new BytesRef(16);
    }

    void reset(int numTerms, int flags, int[] prefixLengths, int[] suffixLengths, int[] termFreqs, int[] positionIndex, int[] positions, int[] startOffsets, int[] lengths,
        int[] payloadIndex, BytesRef payloads, ByteArrayDataInput in) {
      this.numTerms = numTerms;
      this.prefixLengths = prefixLengths;
      this.suffixLengths = suffixLengths;
      this.termFreqs = termFreqs;
      this.positionIndex = positionIndex;
      this.positions = positions;
      this.startOffsets = startOffsets;
      this.lengths = lengths;
      this.payloadIndex = payloadIndex;
      this.payloads = payloads;
      this.in = in;
      startPos = in.getPosition();
      reset();
    }

    void reset() {
      term.length = 0;
      in.setPosition(startPos);
      ord = -1;
    }

    @Override
    public BytesRef next() throws IOException {
      if (ord == numTerms - 1) {
        return null;
      } else {
        assert ord < numTerms;
        ++ord;
      }

      // read term
      term.offset = 0;
      term.length = prefixLengths[ord] + suffixLengths[ord];
      if (term.length > term.bytes.length) {
        term.bytes = ArrayUtil.grow(term.bytes, term.length);
      }
      in.readBytes(term.bytes, prefixLengths[ord], suffixLengths[ord]);

      return term;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text)
        throws IOException {
      if (ord < numTerms && ord >= 0) {
        final int cmp = term().compareTo(text);
        if (cmp == 0) {
          return SeekStatus.FOUND;
        } else if (cmp > 0) {
          reset();
        }
      }
      // linear scan
      while (true) {
        final BytesRef term = next();
        if (term == null) {
          return SeekStatus.END;
        }
        final int cmp = term.compareTo(text);
        if (cmp > 0) {
          return SeekStatus.NOT_FOUND;
        } else if (cmp == 0) {
          return SeekStatus.FOUND;
        }
      }
    }

    @Override
    public void seekExact(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef term() throws IOException {
      return term;
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() throws IOException {
      return 1;
    }

    @Override
    public long totalTermFreq() throws IOException {
      return termFreqs[ord];
    }

    @Override
    public final PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      if (PostingsEnum.featureRequested(flags, DocsAndPositionsEnum.OLD_NULL_SEMANTICS)) {
        if (positions == null && startOffsets == null) {
          // Positions nor offsets were indexed:
          return null;
        }
      }
      
      final TVPostingsEnum docsEnum;
      if (reuse != null && reuse instanceof TVPostingsEnum) {
        docsEnum = (TVPostingsEnum) reuse;
      } else {
        docsEnum = new TVPostingsEnum();
      }

      docsEnum.reset(termFreqs[ord], positionIndex[ord], positions, startOffsets, lengths, payloads, payloadIndex);
      return docsEnum;
    }

  }

  private static class TVPostingsEnum extends PostingsEnum {

    private int doc = -1;
    private int termFreq;
    private int positionIndex;
    private int[] positions;
    private int[] startOffsets;
    private int[] lengths;
    private final BytesRef payload;
    private int[] payloadIndex;
    private int basePayloadOffset;
    private int i;

    TVPostingsEnum() {
      payload = new BytesRef();
    }

    public void reset(int freq, int positionIndex, int[] positions,
        int[] startOffsets, int[] lengths, BytesRef payloads,
        int[] payloadIndex) {
      this.termFreq = freq;
      this.positionIndex = positionIndex;
      this.positions = positions;
      this.startOffsets = startOffsets;
      this.lengths = lengths;
      this.basePayloadOffset = payloads.offset;
      this.payload.bytes = payloads.bytes;
      payload.offset = payload.length = 0;
      this.payloadIndex = payloadIndex;

      doc = i = -1;
    }

    private void checkDoc() {
      if (doc == NO_MORE_DOCS) {
        throw new IllegalStateException("DocsEnum exhausted");
      } else if (doc == -1) {
        throw new IllegalStateException("DocsEnum not started");
      }
    }

    private void checkPosition() {
      checkDoc();
      if (i < 0) {
        throw new IllegalStateException("Position enum not started");
      } else if (i >= termFreq) {
        throw new IllegalStateException("Read past last position");
      }
    }

    @Override
    public int nextPosition() throws IOException {
      if (doc != 0) {
        throw new IllegalStateException();
      } else if (i >= termFreq - 1) {
        throw new IllegalStateException("Read past last position");
      }

      ++i;

      if (payloadIndex != null) {
        payload.offset = basePayloadOffset + payloadIndex[positionIndex + i];
        payload.length = payloadIndex[positionIndex + i + 1] - payloadIndex[positionIndex + i];
      }

      if (positions == null) {
        return -1;
      } else {
        return positions[positionIndex + i];
      }
    }

    @Override
    public int startOffset() throws IOException {
      checkPosition();
      if (startOffsets == null) {
        return -1;
      } else {
        return startOffsets[positionIndex + i];
      }
    }

    @Override
    public int endOffset() throws IOException {
      checkPosition();
      if (startOffsets == null) {
        return -1;
      } else {
        return startOffsets[positionIndex + i] + lengths[positionIndex + i];
      }
    }

    @Override
    public BytesRef getPayload() throws IOException {
      checkPosition();
      if (payloadIndex == null || payload.length == 0) {
        return null;
      } else {
        return payload;
      }
    }

    @Override
    public int freq() throws IOException {
      checkDoc();
      return termFreq;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      if (doc == -1) {
        return (doc = 0);
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      return 1;
    }
  }

  private static int sum(int[] arr) {
    int sum = 0;
    for (int el : arr) {
      sum += el;
    }
    return sum;
  }

  @Override
  public long ramBytesUsed() {
    return indexReader.ramBytesUsed();
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.singleton(Accountables.namedAccountable("term vector index", indexReader));
  }
  
  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(vectorsStream);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(mode=" + compressionMode + ",chunksize=" + chunkSize + ")";
  }
}
