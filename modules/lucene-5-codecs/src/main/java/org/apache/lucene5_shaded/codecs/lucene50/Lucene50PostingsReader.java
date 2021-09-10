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
package org.apache.lucene5_shaded.codecs.lucene50;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene5_shaded.codecs.BlockTermState;
import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.PostingsReaderBase;
import org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat.IntBlockTermState;
import org.apache.lucene5_shaded.index.DocsAndPositionsEnum;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.IndexOptions;
import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.SegmentReadState;
import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.IOUtils;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

import static org.apache.lucene5_shaded.codecs.lucene50.ForUtil.MAX_DATA_SIZE;
import static org.apache.lucene5_shaded.codecs.lucene50.ForUtil.MAX_ENCODED_SIZE;
import static org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat.BLOCK_SIZE;
import static org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat.DOC_CODEC;
import static org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat.MAX_SKIP_LEVELS;
import static org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat.PAY_CODEC;
import static org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat.POS_CODEC;
import static org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat.TERMS_CODEC;
import static org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat.VERSION_CURRENT;
import static org.apache.lucene5_shaded.codecs.lucene50.Lucene50PostingsFormat.VERSION_START;

/**
 * Concrete class that reads docId(maybe frq,pos,offset,payloads) list
 * with postings format.
 *
 * @lucene.experimental
 */
public final class Lucene50PostingsReader extends PostingsReaderBase {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Lucene50PostingsReader.class);

  private final IndexInput docIn;
  private final IndexInput posIn;
  private final IndexInput payIn;

  final ForUtil forUtil;
  private int version;

  /** Sole constructor. */
  public Lucene50PostingsReader(SegmentReadState state) throws IOException {
    boolean success = false;
    IndexInput docIn = null;
    IndexInput posIn = null;
    IndexInput payIn = null;
    
    // NOTE: these data files are too costly to verify checksum against all the bytes on open,
    // but for now we at least verify proper structure of the checksum footer: which looks
    // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
    // such as file truncation.
    
    String docName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene50PostingsFormat.DOC_EXTENSION);
    try {
      docIn = state.directory.openInput(docName, state.context);
      version = CodecUtil.checkIndexHeader(docIn, DOC_CODEC, VERSION_START, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      forUtil = new ForUtil(docIn);
      CodecUtil.retrieveChecksum(docIn);

      if (state.fieldInfos.hasProx()) {
        String proxName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene50PostingsFormat.POS_EXTENSION);
        posIn = state.directory.openInput(proxName, state.context);
        CodecUtil.checkIndexHeader(posIn, POS_CODEC, version, version, state.segmentInfo.getId(), state.segmentSuffix);
        CodecUtil.retrieveChecksum(posIn);

        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          String payName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene50PostingsFormat.PAY_EXTENSION);
          payIn = state.directory.openInput(payName, state.context);
          CodecUtil.checkIndexHeader(payIn, PAY_CODEC, version, version, state.segmentInfo.getId(), state.segmentSuffix);
          CodecUtil.retrieveChecksum(payIn);
        }
      }

      this.docIn = docIn;
      this.posIn = posIn;
      this.payIn = payIn;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docIn, posIn, payIn);
      }
    }
  }

  @Override
  public void init(IndexInput termsIn, SegmentReadState state) throws IOException {
    // Make sure we are talking to the matching postings writer
    CodecUtil.checkIndexHeader(termsIn, TERMS_CODEC, VERSION_START, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
    final int indexBlockSize = termsIn.readVInt();
    if (indexBlockSize != BLOCK_SIZE) {
      throw new IllegalStateException("index-time BLOCK_SIZE (" + indexBlockSize + ") != read-time BLOCK_SIZE (" + BLOCK_SIZE + ")");
    }
  }

  /**
   * Read values that have been written using variable-length encoding instead of bit-packing.
   */
  static void readVIntBlock(IndexInput docIn, int[] docBuffer,
      int[] freqBuffer, int num, boolean indexHasFreq) throws IOException {
    if (indexHasFreq) {
      for(int i=0;i<num;i++) {
        final int code = docIn.readVInt();
        docBuffer[i] = code >>> 1;
        if ((code & 1) != 0) {
          freqBuffer[i] = 1;
        } else {
          freqBuffer[i] = docIn.readVInt();
        }
      }
    } else {
      for(int i=0;i<num;i++) {
        docBuffer[i] = docIn.readVInt();
      }
    }
  }

  @Override
  public BlockTermState newTermState() {
    return new IntBlockTermState();
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(docIn, posIn, payIn);
  }

  @Override
  public void decodeTerm(long[] longs, DataInput in, FieldInfo fieldInfo, BlockTermState _termState, boolean absolute)
    throws IOException {
    final IntBlockTermState termState = (IntBlockTermState) _termState;
    final boolean fieldHasPositions = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    final boolean fieldHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    final boolean fieldHasPayloads = fieldInfo.hasPayloads();

    if (absolute) {
      termState.docStartFP = 0;
      termState.posStartFP = 0;
      termState.payStartFP = 0;
    }

    termState.docStartFP += longs[0];
    if (fieldHasPositions) {
      termState.posStartFP += longs[1];
      if (fieldHasOffsets || fieldHasPayloads) {
        termState.payStartFP += longs[2];
      }
    }
    if (termState.docFreq == 1) {
      termState.singletonDocID = in.readVInt();
    } else {
      termState.singletonDocID = -1;
    }
    if (fieldHasPositions) {
      if (termState.totalTermFreq > BLOCK_SIZE) {
        termState.lastPosBlockOffset = in.readVLong();
      } else {
        termState.lastPosBlockOffset = -1;
      }
    }
    if (termState.docFreq > BLOCK_SIZE) {
      termState.skipOffset = in.readVLong();
    } else {
      termState.skipOffset = -1;
    }
  }
    
  @Override
  public PostingsEnum postings(FieldInfo fieldInfo, BlockTermState termState, PostingsEnum reuse, int flags) throws IOException {
    
    boolean indexHasPositions = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    boolean indexHasPayloads = fieldInfo.hasPayloads();
    
    if (PostingsEnum.featureRequested(flags, DocsAndPositionsEnum.OLD_NULL_SEMANTICS)) {
      if (!indexHasPositions) {
        // Positions were not indexed:
        return null;
      }
    }

    if (indexHasPositions == false || PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS) == false) {
      BlockDocsEnum docsEnum;
      if (reuse instanceof BlockDocsEnum) {
        docsEnum = (BlockDocsEnum) reuse;
        if (!docsEnum.canReuse(docIn, fieldInfo)) {
          docsEnum = new BlockDocsEnum(fieldInfo);
        }
      } else {
        docsEnum = new BlockDocsEnum(fieldInfo);
      }
      return docsEnum.reset((IntBlockTermState) termState, flags);
    } else if ((indexHasOffsets == false || PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS) == false) &&
               (indexHasPayloads == false || PostingsEnum.featureRequested(flags, PostingsEnum.PAYLOADS) == false)) {
      BlockPostingsEnum docsAndPositionsEnum;
      if (reuse instanceof BlockPostingsEnum) {
        docsAndPositionsEnum = (BlockPostingsEnum) reuse;
        if (!docsAndPositionsEnum.canReuse(docIn, fieldInfo)) {
          docsAndPositionsEnum = new BlockPostingsEnum(fieldInfo);
        }
      } else {
        docsAndPositionsEnum = new BlockPostingsEnum(fieldInfo);
      }
      return docsAndPositionsEnum.reset((IntBlockTermState) termState);
    } else {
      EverythingEnum everythingEnum;
      if (reuse instanceof EverythingEnum) {
        everythingEnum = (EverythingEnum) reuse;
        if (!everythingEnum.canReuse(docIn, fieldInfo)) {
          everythingEnum = new EverythingEnum(fieldInfo);
        }
      } else {
        everythingEnum = new EverythingEnum(fieldInfo);
      }
      return everythingEnum.reset((IntBlockTermState) termState, flags);
    }
  }

  final class BlockDocsEnum extends PostingsEnum {
    private final byte[] encoded;
    
    private final int[] docDeltaBuffer = new int[MAX_DATA_SIZE];
    private final int[] freqBuffer = new int[MAX_DATA_SIZE];

    private int docBufferUpto;

    private Lucene50SkipReader skipper;
    private boolean skipped;

    final IndexInput startDocIn;

    IndexInput docIn;
    final boolean indexHasFreq;
    final boolean indexHasPos;
    final boolean indexHasOffsets;
    final boolean indexHasPayloads;

    private int docFreq;                              // number of docs in this posting list
    private long totalTermFreq;                       // sum of freqs in this posting list (or docFreq when omitted)
    private int docUpto;                              // how many docs we've read
    private int doc;                                  // doc we last read
    private int accum;                                // accumulator for doc deltas
    private int freq;                                 // freq we last read

    // Where this term's postings start in the .doc file:
    private long docTermStartFP;

    // Where this term's skip data starts (after
    // docTermStartFP) in the .doc file (or -1 if there is
    // no skip data for this term):
    private long skipOffset;

    // docID for next skip point, we won't use skipper if 
    // target docID is not larger than this
    private int nextSkipDoc;
    
    private boolean needsFreq; // true if the caller actually needs frequencies
    private int singletonDocID; // docid when there is a single pulsed posting, otherwise -1

    public BlockDocsEnum(FieldInfo fieldInfo) throws IOException {
      this.startDocIn = Lucene50PostingsReader.this.docIn;
      this.docIn = null;
      indexHasFreq = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
      indexHasPos = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      indexHasPayloads = fieldInfo.hasPayloads();
      encoded = new byte[MAX_ENCODED_SIZE];    
    }

    public boolean canReuse(IndexInput docIn, FieldInfo fieldInfo) {
      return docIn == startDocIn &&
        indexHasFreq == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) &&
        indexHasPos == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) &&
        indexHasPayloads == fieldInfo.hasPayloads();
    }
    
    public PostingsEnum reset(IntBlockTermState termState, int flags) throws IOException {
      docFreq = termState.docFreq;
      totalTermFreq = indexHasFreq ? termState.totalTermFreq : docFreq;
      docTermStartFP = termState.docStartFP;
      skipOffset = termState.skipOffset;
      singletonDocID = termState.singletonDocID;
      if (docFreq > 1) {
        if (docIn == null) {
          // lazy init
          docIn = startDocIn.clone();
        }
        docIn.seek(docTermStartFP);
      }

      doc = -1;
      this.needsFreq = PostingsEnum.featureRequested(flags, PostingsEnum.FREQS);
      if (indexHasFreq == false || needsFreq == false) {
        Arrays.fill(freqBuffer, 1);
      }
      accum = 0;
      docUpto = 0;
      nextSkipDoc = BLOCK_SIZE - 1; // we won't skip if target is found in first block
      docBufferUpto = BLOCK_SIZE;
      skipped = false;
      return this;
    }
    
    @Override
    public int freq() throws IOException {
      return freq;
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

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public int docID() {
      return doc;
    }
    
    private void refillDocs() throws IOException {
      final int left = docFreq - docUpto;
      assert left > 0;

      if (left >= BLOCK_SIZE) {
        forUtil.readBlock(docIn, encoded, docDeltaBuffer);

        if (indexHasFreq) {
          if (needsFreq) {
            forUtil.readBlock(docIn, encoded, freqBuffer);
          } else {
            forUtil.skipBlock(docIn); // skip over freqs
          }
        }
      } else if (docFreq == 1) {
        docDeltaBuffer[0] = singletonDocID;
        freqBuffer[0] = (int) totalTermFreq;
      } else {
        // Read vInts:
        readVIntBlock(docIn, docDeltaBuffer, freqBuffer, left, indexHasFreq);
      }
      docBufferUpto = 0;
    }

    @Override
    public int nextDoc() throws IOException {
      if (docUpto == docFreq) {
        return doc = NO_MORE_DOCS;
      }
      if (docBufferUpto == BLOCK_SIZE) {
        refillDocs();
      }

      accum += docDeltaBuffer[docBufferUpto];
      docUpto++;

      doc = accum;
      freq = freqBuffer[docBufferUpto];
      docBufferUpto++;
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      // TODO: make frq block load lazy/skippable

      // current skip docID < docIDs generated from current buffer <= next skip docID
      // we don't need to skip if target is buffered already
      if (docFreq > BLOCK_SIZE && target > nextSkipDoc) {

        if (skipper == null) {
          // Lazy init: first time this enum has ever been used for skipping
          skipper = new Lucene50SkipReader(docIn.clone(),
                                           MAX_SKIP_LEVELS,
                                           indexHasPos,
                                           indexHasOffsets,
                                           indexHasPayloads);
        }

        if (!skipped) {
          assert skipOffset != -1;
          // This is the first time this enum has skipped
          // since reset() was called; load the skip data:
          skipper.init(docTermStartFP+skipOffset, docTermStartFP, 0, 0, docFreq);
          skipped = true;
        }

        // always plus one to fix the result, since skip position in Lucene50SkipReader 
        // is a little different from MultiLevelSkipListReader
        final int newDocUpto = skipper.skipTo(target) + 1; 

        if (newDocUpto > docUpto) {
          // Skipper moved
          assert newDocUpto % BLOCK_SIZE == 0 : "got " + newDocUpto;
          docUpto = newDocUpto;

          // Force to read next block
          docBufferUpto = BLOCK_SIZE;
          accum = skipper.getDoc();               // actually, this is just lastSkipEntry
          docIn.seek(skipper.getDocPointer());    // now point to the block we want to search
        }
        // next time we call advance, this is used to 
        // foresee whether skipper is necessary.
        nextSkipDoc = skipper.getNextSkipDoc();
      }
      if (docUpto == docFreq) {
        return doc = NO_MORE_DOCS;
      }
      if (docBufferUpto == BLOCK_SIZE) {
        refillDocs();
      }

      // Now scan... this is an inlined/pared down version
      // of nextDoc():
      while (true) {
        accum += docDeltaBuffer[docBufferUpto];
        docUpto++;

        if (accum >= target) {
          break;
        }
        docBufferUpto++;
        if (docUpto == docFreq) {
          return doc = NO_MORE_DOCS;
        }
      }

      freq = freqBuffer[docBufferUpto];
      docBufferUpto++;
      return doc = accum;
    }
    
    @Override
    public long cost() {
      return docFreq;
    }
  }


  final class BlockPostingsEnum extends PostingsEnum {
    
    private final byte[] encoded;

    private final int[] docDeltaBuffer = new int[MAX_DATA_SIZE];
    private final int[] freqBuffer = new int[MAX_DATA_SIZE];
    private final int[] posDeltaBuffer = new int[MAX_DATA_SIZE];

    private int docBufferUpto;
    private int posBufferUpto;

    private Lucene50SkipReader skipper;
    private boolean skipped;

    final IndexInput startDocIn;

    IndexInput docIn;
    final IndexInput posIn;

    final boolean indexHasOffsets;
    final boolean indexHasPayloads;

    private int docFreq;                              // number of docs in this posting list
    private long totalTermFreq;                       // number of positions in this posting list
    private int docUpto;                              // how many docs we've read
    private int doc;                                  // doc we last read
    private int accum;                                // accumulator for doc deltas
    private int freq;                                 // freq we last read
    private int position;                             // current position

    // how many positions "behind" we are; nextPosition must
    // skip these to "catch up":
    private int posPendingCount;

    // Lazy pos seek: if != -1 then we must seek to this FP
    // before reading positions:
    private long posPendingFP;

    // Where this term's postings start in the .doc file:
    private long docTermStartFP;

    // Where this term's postings start in the .pos file:
    private long posTermStartFP;

    // Where this term's payloads/offsets start in the .pay
    // file:
    private long payTermStartFP;

    // File pointer where the last (vInt encoded) pos delta
    // block is.  We need this to know whether to bulk
    // decode vs vInt decode the block:
    private long lastPosBlockFP;

    // Where this term's skip data starts (after
    // docTermStartFP) in the .doc file (or -1 if there is
    // no skip data for this term):
    private long skipOffset;

    private int nextSkipDoc;

    private int singletonDocID; // docid when there is a single pulsed posting, otherwise -1
    
    public BlockPostingsEnum(FieldInfo fieldInfo) throws IOException {
      this.startDocIn = Lucene50PostingsReader.this.docIn;
      this.docIn = null;
      this.posIn = Lucene50PostingsReader.this.posIn.clone();
      encoded = new byte[MAX_ENCODED_SIZE];
      indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      indexHasPayloads = fieldInfo.hasPayloads();
    }

    public boolean canReuse(IndexInput docIn, FieldInfo fieldInfo) {
      return docIn == startDocIn &&
        indexHasOffsets == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) &&
        indexHasPayloads == fieldInfo.hasPayloads();
    }
    
    public PostingsEnum reset(IntBlockTermState termState) throws IOException {
      docFreq = termState.docFreq;
      docTermStartFP = termState.docStartFP;
      posTermStartFP = termState.posStartFP;
      payTermStartFP = termState.payStartFP;
      skipOffset = termState.skipOffset;
      totalTermFreq = termState.totalTermFreq;
      singletonDocID = termState.singletonDocID;
      if (docFreq > 1) {
        if (docIn == null) {
          // lazy init
          docIn = startDocIn.clone();
        }
        docIn.seek(docTermStartFP);
      }
      posPendingFP = posTermStartFP;
      posPendingCount = 0;
      if (termState.totalTermFreq < BLOCK_SIZE) {
        lastPosBlockFP = posTermStartFP;
      } else if (termState.totalTermFreq == BLOCK_SIZE) {
        lastPosBlockFP = -1;
      } else {
        lastPosBlockFP = posTermStartFP + termState.lastPosBlockOffset;
      }

      doc = -1;
      accum = 0;
      docUpto = 0;
      if (docFreq > BLOCK_SIZE) {
        nextSkipDoc = BLOCK_SIZE - 1; // we won't skip if target is found in first block
      } else {
        nextSkipDoc = NO_MORE_DOCS; // not enough docs for skipping
      }
      docBufferUpto = BLOCK_SIZE;
      skipped = false;
      return this;
    }
    
    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int docID() {
      return doc;
    }

    private void refillDocs() throws IOException {
      final int left = docFreq - docUpto;
      assert left > 0;

      if (left >= BLOCK_SIZE) {
        forUtil.readBlock(docIn, encoded, docDeltaBuffer);
        forUtil.readBlock(docIn, encoded, freqBuffer);
      } else if (docFreq == 1) {
        docDeltaBuffer[0] = singletonDocID;
        freqBuffer[0] = (int) totalTermFreq;
      } else {
        // Read vInts:
        readVIntBlock(docIn, docDeltaBuffer, freqBuffer, left, true);
      }
      docBufferUpto = 0;
    }
    
    private void refillPositions() throws IOException {
      if (posIn.getFilePointer() == lastPosBlockFP) {
        final int count = (int) (totalTermFreq % BLOCK_SIZE);
        int payloadLength = 0;
        for(int i=0;i<count;i++) {
          int code = posIn.readVInt();
          if (indexHasPayloads) {
            if ((code & 1) != 0) {
              payloadLength = posIn.readVInt();
            }
            posDeltaBuffer[i] = code >>> 1;
            if (payloadLength != 0) {
              posIn.seek(posIn.getFilePointer() + payloadLength);
            }
          } else {
            posDeltaBuffer[i] = code;
          }
          if (indexHasOffsets) {
            if ((posIn.readVInt() & 1) != 0) {
              // offset length changed
              posIn.readVInt();
            }
          }
        }
      } else {
        forUtil.readBlock(posIn, encoded, posDeltaBuffer);
      }
    }

    @Override
    public int nextDoc() throws IOException {
      if (docUpto == docFreq) {
        return doc = NO_MORE_DOCS;
      }
      if (docBufferUpto == BLOCK_SIZE) {
        refillDocs();
      }

      accum += docDeltaBuffer[docBufferUpto];
      freq = freqBuffer[docBufferUpto];
      posPendingCount += freq;
      docBufferUpto++;
      docUpto++;

      doc = accum;
      position = 0;
      return doc;
    }
    
    @Override
    public int advance(int target) throws IOException {
      // TODO: make frq block load lazy/skippable

      if (target > nextSkipDoc) {
        if (skipper == null) {
          // Lazy init: first time this enum has ever been used for skipping
          skipper = new Lucene50SkipReader(docIn.clone(),
                                           MAX_SKIP_LEVELS,
                                           true,
                                           indexHasOffsets,
                                           indexHasPayloads);
        }

        if (!skipped) {
          assert skipOffset != -1;
          // This is the first time this enum has skipped
          // since reset() was called; load the skip data:
          skipper.init(docTermStartFP+skipOffset, docTermStartFP, posTermStartFP, payTermStartFP, docFreq);
          skipped = true;
        }

        final int newDocUpto = skipper.skipTo(target) + 1; 

        if (newDocUpto > docUpto) {
          // Skipper moved

          assert newDocUpto % BLOCK_SIZE == 0 : "got " + newDocUpto;
          docUpto = newDocUpto;

          // Force to read next block
          docBufferUpto = BLOCK_SIZE;
          accum = skipper.getDoc();
          docIn.seek(skipper.getDocPointer());
          posPendingFP = skipper.getPosPointer();
          posPendingCount = skipper.getPosBufferUpto();
        }
        nextSkipDoc = skipper.getNextSkipDoc();
      }
      if (docUpto == docFreq) {
        return doc = NO_MORE_DOCS;
      }
      if (docBufferUpto == BLOCK_SIZE) {
        refillDocs();
      }

      // Now scan... this is an inlined/pared down version
      // of nextDoc():
      while (true) {
        accum += docDeltaBuffer[docBufferUpto];
        freq = freqBuffer[docBufferUpto];
        posPendingCount += freq;
        docBufferUpto++;
        docUpto++;

        if (accum >= target) {
          break;
        }
        if (docUpto == docFreq) {
          return doc = NO_MORE_DOCS;
        }
      }

      position = 0;
      return doc = accum;
    }

    // TODO: in theory we could avoid loading frq block
    // when not needed, ie, use skip data to load how far to
    // seek the pos pointer ... instead of having to load frq
    // blocks only to sum up how many positions to skip
    private void skipPositions() throws IOException {
      // Skip positions now:
      int toSkip = posPendingCount - freq;

      final int leftInBlock = BLOCK_SIZE - posBufferUpto;
      if (toSkip < leftInBlock) {
        posBufferUpto += toSkip;
      } else {
        toSkip -= leftInBlock;
        while(toSkip >= BLOCK_SIZE) {
          assert posIn.getFilePointer() != lastPosBlockFP;
          forUtil.skipBlock(posIn);
          toSkip -= BLOCK_SIZE;
        }
        refillPositions();
        posBufferUpto = toSkip;
      }

      position = 0;
    }

    @Override
    public int nextPosition() throws IOException {

      assert posPendingCount > 0;

      if (posPendingFP != -1) {
        posIn.seek(posPendingFP);
        posPendingFP = -1;

        // Force buffer refill:
        posBufferUpto = BLOCK_SIZE;
      }

      if (posPendingCount > freq) {
        skipPositions();
        posPendingCount = freq;
      }

      if (posBufferUpto == BLOCK_SIZE) {
        refillPositions();
        posBufferUpto = 0;
      }
      position += posDeltaBuffer[posBufferUpto++];
      posPendingCount--;
      return position;
    }

    @Override
    public int startOffset() {
      return -1;
    }
  
    @Override
    public int endOffset() {
      return -1;
    }
  
    @Override
    public BytesRef getPayload() {
      return null;
    }
    
    @Override
    public long cost() {
      return docFreq;
    }
  }

  // Also handles payloads + offsets
  final class EverythingEnum extends PostingsEnum {
    
    private final byte[] encoded;

    private final int[] docDeltaBuffer = new int[MAX_DATA_SIZE];
    private final int[] freqBuffer = new int[MAX_DATA_SIZE];
    private final int[] posDeltaBuffer = new int[MAX_DATA_SIZE];

    private final int[] payloadLengthBuffer;
    private final int[] offsetStartDeltaBuffer;
    private final int[] offsetLengthBuffer;

    private byte[] payloadBytes;
    private int payloadByteUpto;
    private int payloadLength;

    private int lastStartOffset;
    private int startOffset;
    private int endOffset;

    private int docBufferUpto;
    private int posBufferUpto;

    private Lucene50SkipReader skipper;
    private boolean skipped;

    final IndexInput startDocIn;

    IndexInput docIn;
    final IndexInput posIn;
    final IndexInput payIn;
    final BytesRef payload;

    final boolean indexHasOffsets;
    final boolean indexHasPayloads;

    private int docFreq;                              // number of docs in this posting list
    private long totalTermFreq;                       // number of positions in this posting list
    private int docUpto;                              // how many docs we've read
    private int doc;                                  // doc we last read
    private int accum;                                // accumulator for doc deltas
    private int freq;                                 // freq we last read
    private int position;                             // current position

    // how many positions "behind" we are; nextPosition must
    // skip these to "catch up":
    private int posPendingCount;

    // Lazy pos seek: if != -1 then we must seek to this FP
    // before reading positions:
    private long posPendingFP;

    // Lazy pay seek: if != -1 then we must seek to this FP
    // before reading payloads/offsets:
    private long payPendingFP;

    // Where this term's postings start in the .doc file:
    private long docTermStartFP;

    // Where this term's postings start in the .pos file:
    private long posTermStartFP;

    // Where this term's payloads/offsets start in the .pay
    // file:
    private long payTermStartFP;

    // File pointer where the last (vInt encoded) pos delta
    // block is.  We need this to know whether to bulk
    // decode vs vInt decode the block:
    private long lastPosBlockFP;

    // Where this term's skip data starts (after
    // docTermStartFP) in the .doc file (or -1 if there is
    // no skip data for this term):
    private long skipOffset;

    private int nextSkipDoc;
    
    private boolean needsOffsets; // true if we actually need offsets
    private boolean needsPayloads; // true if we actually need payloads
    private int singletonDocID; // docid when there is a single pulsed posting, otherwise -1
    
    public EverythingEnum(FieldInfo fieldInfo) throws IOException {
      this.startDocIn = Lucene50PostingsReader.this.docIn;
      this.docIn = null;
      this.posIn = Lucene50PostingsReader.this.posIn.clone();
      this.payIn = Lucene50PostingsReader.this.payIn.clone();
      encoded = new byte[MAX_ENCODED_SIZE];
      indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      if (indexHasOffsets) {
        offsetStartDeltaBuffer = new int[MAX_DATA_SIZE];
        offsetLengthBuffer = new int[MAX_DATA_SIZE];
      } else {
        offsetStartDeltaBuffer = null;
        offsetLengthBuffer = null;
        startOffset = -1;
        endOffset = -1;
      }

      indexHasPayloads = fieldInfo.hasPayloads();
      if (indexHasPayloads) {
        payloadLengthBuffer = new int[MAX_DATA_SIZE];
        payloadBytes = new byte[128];
        payload = new BytesRef();
      } else {
        payloadLengthBuffer = null;
        payloadBytes = null;
        payload = null;
      }
    }

    public boolean canReuse(IndexInput docIn, FieldInfo fieldInfo) {
      return docIn == startDocIn &&
        indexHasOffsets == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) &&
        indexHasPayloads == fieldInfo.hasPayloads();
    }
    
    public EverythingEnum reset(IntBlockTermState termState, int flags) throws IOException {
      docFreq = termState.docFreq;
      docTermStartFP = termState.docStartFP;
      posTermStartFP = termState.posStartFP;
      payTermStartFP = termState.payStartFP;
      skipOffset = termState.skipOffset;
      totalTermFreq = termState.totalTermFreq;
      singletonDocID = termState.singletonDocID;
      if (docFreq > 1) {
        if (docIn == null) {
          // lazy init
          docIn = startDocIn.clone();
        }
        docIn.seek(docTermStartFP);
      }
      posPendingFP = posTermStartFP;
      payPendingFP = payTermStartFP;
      posPendingCount = 0;
      if (termState.totalTermFreq < BLOCK_SIZE) {
        lastPosBlockFP = posTermStartFP;
      } else if (termState.totalTermFreq == BLOCK_SIZE) {
        lastPosBlockFP = -1;
      } else {
        lastPosBlockFP = posTermStartFP + termState.lastPosBlockOffset;
      }

      this.needsOffsets = PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS);
      this.needsPayloads = PostingsEnum.featureRequested(flags, PostingsEnum.PAYLOADS);

      doc = -1;
      accum = 0;
      docUpto = 0;
      if (docFreq > BLOCK_SIZE) {
        nextSkipDoc = BLOCK_SIZE - 1; // we won't skip if target is found in first block
      } else {
        nextSkipDoc = NO_MORE_DOCS; // not enough docs for skipping
      }
      docBufferUpto = BLOCK_SIZE;
      skipped = false;
      return this;
    }
    
    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int docID() {
      return doc;
    }

    private void refillDocs() throws IOException {
      final int left = docFreq - docUpto;
      assert left > 0;

      if (left >= BLOCK_SIZE) {
        forUtil.readBlock(docIn, encoded, docDeltaBuffer);
        forUtil.readBlock(docIn, encoded, freqBuffer);
      } else if (docFreq == 1) {
        docDeltaBuffer[0] = singletonDocID;
        freqBuffer[0] = (int) totalTermFreq;
      } else {
        readVIntBlock(docIn, docDeltaBuffer, freqBuffer, left, true);
      }
      docBufferUpto = 0;
    }
    
    private void refillPositions() throws IOException {
      if (posIn.getFilePointer() == lastPosBlockFP) {
        final int count = (int) (totalTermFreq % BLOCK_SIZE);
        int payloadLength = 0;
        int offsetLength = 0;
        payloadByteUpto = 0;
        for(int i=0;i<count;i++) {
          int code = posIn.readVInt();
          if (indexHasPayloads) {
            if ((code & 1) != 0) {
              payloadLength = posIn.readVInt();
            }
            payloadLengthBuffer[i] = payloadLength;
            posDeltaBuffer[i] = code >>> 1;
            if (payloadLength != 0) {
              if (payloadByteUpto + payloadLength > payloadBytes.length) {
                payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payloadLength);
              }
              posIn.readBytes(payloadBytes, payloadByteUpto, payloadLength);
              payloadByteUpto += payloadLength;
            }
          } else {
            posDeltaBuffer[i] = code;
          }

          if (indexHasOffsets) {
            int deltaCode = posIn.readVInt();
            if ((deltaCode & 1) != 0) {
              offsetLength = posIn.readVInt();
            }
            offsetStartDeltaBuffer[i] = deltaCode >>> 1;
            offsetLengthBuffer[i] = offsetLength;
          }
        }
        payloadByteUpto = 0;
      } else {
        forUtil.readBlock(posIn, encoded, posDeltaBuffer);

        if (indexHasPayloads) {
          if (needsPayloads) {
            forUtil.readBlock(payIn, encoded, payloadLengthBuffer);
            int numBytes = payIn.readVInt();

            if (numBytes > payloadBytes.length) {
              payloadBytes = ArrayUtil.grow(payloadBytes, numBytes);
            }
            payIn.readBytes(payloadBytes, 0, numBytes);
          } else {
            // this works, because when writing a vint block we always force the first length to be written
            forUtil.skipBlock(payIn); // skip over lengths
            int numBytes = payIn.readVInt(); // read length of payloadBytes
            payIn.seek(payIn.getFilePointer() + numBytes); // skip over payloadBytes
          }
          payloadByteUpto = 0;
        }

        if (indexHasOffsets) {
          if (needsOffsets) {
            forUtil.readBlock(payIn, encoded, offsetStartDeltaBuffer);
            forUtil.readBlock(payIn, encoded, offsetLengthBuffer);
          } else {
            // this works, because when writing a vint block we always force the first length to be written
            forUtil.skipBlock(payIn); // skip over starts
            forUtil.skipBlock(payIn); // skip over lengths
          }
        }
      }
    }

    @Override
    public int nextDoc() throws IOException {
      if (docUpto == docFreq) {
        return doc = NO_MORE_DOCS;
      }
      if (docBufferUpto == BLOCK_SIZE) {
        refillDocs();
      }

      accum += docDeltaBuffer[docBufferUpto];
      freq = freqBuffer[docBufferUpto];
      posPendingCount += freq;
      docBufferUpto++;
      docUpto++;

      doc = accum;
      position = 0;
      lastStartOffset = 0;
      return doc;
    }
    
    @Override
    public int advance(int target) throws IOException {
      // TODO: make frq block load lazy/skippable

      if (target > nextSkipDoc) {
        if (skipper == null) {
          // Lazy init: first time this enum has ever been used for skipping
          skipper = new Lucene50SkipReader(docIn.clone(),
                                        MAX_SKIP_LEVELS,
                                        true,
                                        indexHasOffsets,
                                        indexHasPayloads);
        }

        if (!skipped) {
          assert skipOffset != -1;
          // This is the first time this enum has skipped
          // since reset() was called; load the skip data:
          skipper.init(docTermStartFP+skipOffset, docTermStartFP, posTermStartFP, payTermStartFP, docFreq);
          skipped = true;
        }

        final int newDocUpto = skipper.skipTo(target) + 1; 

        if (newDocUpto > docUpto) {
          // Skipper moved
          assert newDocUpto % BLOCK_SIZE == 0 : "got " + newDocUpto;
          docUpto = newDocUpto;

          // Force to read next block
          docBufferUpto = BLOCK_SIZE;
          accum = skipper.getDoc();
          docIn.seek(skipper.getDocPointer());
          posPendingFP = skipper.getPosPointer();
          payPendingFP = skipper.getPayPointer();
          posPendingCount = skipper.getPosBufferUpto();
          lastStartOffset = 0; // new document
          payloadByteUpto = skipper.getPayloadByteUpto();
        }
        nextSkipDoc = skipper.getNextSkipDoc();
      }
      if (docUpto == docFreq) {
        return doc = NO_MORE_DOCS;
      }
      if (docBufferUpto == BLOCK_SIZE) {
        refillDocs();
      }

      // Now scan:
      while (true) {
        accum += docDeltaBuffer[docBufferUpto];
        freq = freqBuffer[docBufferUpto];
        posPendingCount += freq;
        docBufferUpto++;
        docUpto++;

        if (accum >= target) {
          break;
        }
        if (docUpto == docFreq) {
          return doc = NO_MORE_DOCS;
        }
      }

      position = 0;
      lastStartOffset = 0;
      return doc = accum;
    }

    // TODO: in theory we could avoid loading frq block
    // when not needed, ie, use skip data to load how far to
    // seek the pos pointer ... instead of having to load frq
    // blocks only to sum up how many positions to skip
    private void skipPositions() throws IOException {
      // Skip positions now:
      int toSkip = posPendingCount - freq;
      // if (DEBUG) {
      //   System.out.println("      FPR.skipPositions: toSkip=" + toSkip);
      // }

      final int leftInBlock = BLOCK_SIZE - posBufferUpto;
      if (toSkip < leftInBlock) {
        int end = posBufferUpto + toSkip;
        while(posBufferUpto < end) {
          if (indexHasPayloads) {
            payloadByteUpto += payloadLengthBuffer[posBufferUpto];
          }
          posBufferUpto++;
        }
      } else {
        toSkip -= leftInBlock;
        while(toSkip >= BLOCK_SIZE) {
          assert posIn.getFilePointer() != lastPosBlockFP;
          forUtil.skipBlock(posIn);

          if (indexHasPayloads) {
            // Skip payloadLength block:
            forUtil.skipBlock(payIn);

            // Skip payloadBytes block:
            int numBytes = payIn.readVInt();
            payIn.seek(payIn.getFilePointer() + numBytes);
          }

          if (indexHasOffsets) {
            forUtil.skipBlock(payIn);
            forUtil.skipBlock(payIn);
          }
          toSkip -= BLOCK_SIZE;
        }
        refillPositions();
        payloadByteUpto = 0;
        posBufferUpto = 0;
        while(posBufferUpto < toSkip) {
          if (indexHasPayloads) {
            payloadByteUpto += payloadLengthBuffer[posBufferUpto];
          }
          posBufferUpto++;
        }
      }

      position = 0;
      lastStartOffset = 0;
    }

    @Override
    public int nextPosition() throws IOException {
      assert posPendingCount > 0;
      
      if (posPendingFP != -1) {
        posIn.seek(posPendingFP);
        posPendingFP = -1;

        if (payPendingFP != -1) {
          payIn.seek(payPendingFP);
          payPendingFP = -1;
        }

        // Force buffer refill:
        posBufferUpto = BLOCK_SIZE;
      }

      if (posPendingCount > freq) {
        skipPositions();
        posPendingCount = freq;
      }

      if (posBufferUpto == BLOCK_SIZE) {
        refillPositions();
        posBufferUpto = 0;
      }
      position += posDeltaBuffer[posBufferUpto];

      if (indexHasPayloads) {
        payloadLength = payloadLengthBuffer[posBufferUpto];
        payload.bytes = payloadBytes;
        payload.offset = payloadByteUpto;
        payload.length = payloadLength;
        payloadByteUpto += payloadLength;
      }

      if (indexHasOffsets) {
        startOffset = lastStartOffset + offsetStartDeltaBuffer[posBufferUpto];
        endOffset = startOffset + offsetLengthBuffer[posBufferUpto];
        lastStartOffset = startOffset;
      }

      posBufferUpto++;
      posPendingCount--;
      return position;
    }

    @Override
    public int startOffset() {
      return startOffset;
    }
  
    @Override
    public int endOffset() {
      return endOffset;
    }
  
    @Override
    public BytesRef getPayload() {
      if (payloadLength == 0) {
        return null;
      } else {
        return payload;
      }
    }
    
    @Override
    public long cost() {
      return docFreq;
    }
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED;
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  @Override
  public void checkIntegrity() throws IOException {
    if (docIn != null) {
      CodecUtil.checksumEntireFile(docIn);
    }
    if (posIn != null) {
      CodecUtil.checksumEntireFile(posIn);
    }
    if (payIn != null) {
      CodecUtil.checksumEntireFile(payIn);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(positions=" + (posIn != null) + ",payloads=" + (payIn != null) +")";
  }
}
