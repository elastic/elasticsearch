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
package org.apache.lucene5_shaded.index;


import java.io.IOException;

import org.apache.lucene5_shaded.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene5_shaded.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene5_shaded.codecs.TermVectorsWriter;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

final class TermVectorsConsumerPerField extends TermsHashPerField {

  private TermVectorsPostingsArray termVectorsPostingsArray;

  final TermVectorsConsumer termsWriter;

  boolean doVectors;
  boolean doVectorPositions;
  boolean doVectorOffsets;
  boolean doVectorPayloads;

  OffsetAttribute offsetAttribute;
  PayloadAttribute payloadAttribute;
  boolean hasPayloads; // if enabled, and we actually saw any for this field

  public TermVectorsConsumerPerField(FieldInvertState invertState, TermVectorsConsumer termsWriter, FieldInfo fieldInfo) {
    super(2, invertState, termsWriter, null, fieldInfo);
    this.termsWriter = termsWriter;
  }

  /** Called once per field per document if term vectors
   *  are enabled, to write the vectors to
   *  RAMOutputStream, which is then quickly flushed to
   *  the real term vectors files in the Directory. */  @Override
  void finish() {
    if (!doVectors || bytesHash.size() == 0) {
      return;
    }
    termsWriter.addFieldToFlush(this);
  }

  void finishDocument() throws IOException {
    if (doVectors == false) {
      return;
    }

    doVectors = false;

    final int numPostings = bytesHash.size();

    final BytesRef flushTerm = termsWriter.flushTerm;

    assert numPostings >= 0;

    // This is called once, after inverting all occurrences
    // of a given field in the doc.  At this point we flush
    // our hash into the DocWriter.

    TermVectorsPostingsArray postings = termVectorsPostingsArray;
    final TermVectorsWriter tv = termsWriter.writer;

    final int[] termIDs = sortPostings();

    tv.startField(fieldInfo, numPostings, doVectorPositions, doVectorOffsets, hasPayloads);
    
    final ByteSliceReader posReader = doVectorPositions ? termsWriter.vectorSliceReaderPos : null;
    final ByteSliceReader offReader = doVectorOffsets ? termsWriter.vectorSliceReaderOff : null;
    
    for(int j=0;j<numPostings;j++) {
      final int termID = termIDs[j];
      final int freq = postings.freqs[termID];

      // Get BytesRef
      termBytePool.setBytesRef(flushTerm, postings.textStarts[termID]);
      tv.startTerm(flushTerm, freq);
      
      if (doVectorPositions || doVectorOffsets) {
        if (posReader != null) {
          initReader(posReader, termID, 0);
        }
        if (offReader != null) {
          initReader(offReader, termID, 1);
        }
        tv.addProx(freq, posReader, offReader);
      }
      tv.finishTerm();
    }
    tv.finishField();

    reset();

    fieldInfo.setStoreTermVectors();
  }

  @Override
  boolean start(IndexableField field, boolean first) {
    assert field.fieldType().indexOptions() != IndexOptions.NONE;

    if (first) {

      if (bytesHash.size() != 0) {
        // Only necessary if previous doc hit a
        // non-aborting exception while writing vectors in
        // this field:
        reset();
      }

      bytesHash.reinit();

      hasPayloads = false;

      doVectors = field.fieldType().storeTermVectors();

      if (doVectors) {

        termsWriter.hasVectors = true;

        doVectorPositions = field.fieldType().storeTermVectorPositions();

        // Somewhat confusingly, unlike postings, you are
        // allowed to index TV offsets without TV positions:
        doVectorOffsets = field.fieldType().storeTermVectorOffsets();

        if (doVectorPositions) {
          doVectorPayloads = field.fieldType().storeTermVectorPayloads();
        } else {
          doVectorPayloads = false;
          if (field.fieldType().storeTermVectorPayloads()) {
            // TODO: move this check somewhere else, and impl the other missing ones
            throw new IllegalArgumentException("cannot index term vector payloads without term vector positions (field=\"" + field.name() + "\")");
          }
        }
        
      } else {
        if (field.fieldType().storeTermVectorOffsets()) {
          throw new IllegalArgumentException("cannot index term vector offsets when term vectors are not indexed (field=\"" + field.name() + "\")");
        }
        if (field.fieldType().storeTermVectorPositions()) {
          throw new IllegalArgumentException("cannot index term vector positions when term vectors are not indexed (field=\"" + field.name() + "\")");
        }
        if (field.fieldType().storeTermVectorPayloads()) {
          throw new IllegalArgumentException("cannot index term vector payloads when term vectors are not indexed (field=\"" + field.name() + "\")");
        }
      }
    } else {
      if (doVectors != field.fieldType().storeTermVectors()) {
        throw new IllegalArgumentException("all instances of a given field name must have the same term vectors settings (storeTermVectors changed for field=\"" + field.name() + "\")");
      }
      if (doVectorPositions != field.fieldType().storeTermVectorPositions()) {
        throw new IllegalArgumentException("all instances of a given field name must have the same term vectors settings (storeTermVectorPositions changed for field=\"" + field.name() + "\")");
      }
      if (doVectorOffsets != field.fieldType().storeTermVectorOffsets()) {
        throw new IllegalArgumentException("all instances of a given field name must have the same term vectors settings (storeTermVectorOffsets changed for field=\"" + field.name() + "\")");
      }
      if (doVectorPayloads != field.fieldType().storeTermVectorPayloads()) {
        throw new IllegalArgumentException("all instances of a given field name must have the same term vectors settings (storeTermVectorPayloads changed for field=\"" + field.name() + "\")");
      }
    }

    if (doVectors) {
      if (doVectorOffsets) {
        offsetAttribute = fieldState.offsetAttribute;
        assert offsetAttribute != null;
      }

      if (doVectorPayloads) {
        // Can be null:
        payloadAttribute = fieldState.payloadAttribute;
      } else {
        payloadAttribute = null;
      }
    }

    return doVectors;
  }
  
  void writeProx(TermVectorsPostingsArray postings, int termID) {    
    if (doVectorOffsets) {
      int startOffset = fieldState.offset + offsetAttribute.startOffset();
      int endOffset = fieldState.offset + offsetAttribute.endOffset();

      writeVInt(1, startOffset - postings.lastOffsets[termID]);
      writeVInt(1, endOffset - startOffset);
      postings.lastOffsets[termID] = endOffset;
    }

    if (doVectorPositions) {
      final BytesRef payload;
      if (payloadAttribute == null) {
        payload = null;
      } else {
        payload = payloadAttribute.getPayload();
      }
      
      final int pos = fieldState.position - postings.lastPositions[termID];
      if (payload != null && payload.length > 0) {
        writeVInt(0, (pos<<1)|1);
        writeVInt(0, payload.length);
        writeBytes(0, payload.bytes, payload.offset, payload.length);
        hasPayloads = true;
      } else {
        writeVInt(0, pos<<1);
      }
      postings.lastPositions[termID] = fieldState.position;
    }
  }

  @Override
  void newTerm(final int termID) {
    TermVectorsPostingsArray postings = termVectorsPostingsArray;

    postings.freqs[termID] = 1;
    postings.lastOffsets[termID] = 0;
    postings.lastPositions[termID] = 0;
    
    writeProx(postings, termID);
  }

  @Override
  void addTerm(final int termID) {
    TermVectorsPostingsArray postings = termVectorsPostingsArray;

    postings.freqs[termID]++;

    writeProx(postings, termID);
  }

  @Override
  public void newPostingsArray() {
    termVectorsPostingsArray = (TermVectorsPostingsArray) postingsArray;
  }

  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    return new TermVectorsPostingsArray(size);
  }

  static final class TermVectorsPostingsArray extends ParallelPostingsArray {
    public TermVectorsPostingsArray(int size) {
      super(size);
      freqs = new int[size];
      lastOffsets = new int[size];
      lastPositions = new int[size];
    }

    int[] freqs;                                       // How many times this term occurred in the current doc
    int[] lastOffsets;                                 // Last offset we saw
    int[] lastPositions;                               // Last position where this term occurred

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new TermVectorsPostingsArray(size);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof TermVectorsPostingsArray;
      TermVectorsPostingsArray to = (TermVectorsPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(freqs, 0, to.freqs, 0, size);
      System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, size);
      System.arraycopy(lastPositions, 0, to.lastPositions, 0, size);
    }

    @Override
    int bytesPerPosting() {
      return super.bytesPerPosting() + 3 * RamUsageEstimator.NUM_BYTES_INT;
    }
  }
}
