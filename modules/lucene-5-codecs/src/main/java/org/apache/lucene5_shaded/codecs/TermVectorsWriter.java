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
package org.apache.lucene5_shaded.codecs;


import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene5_shaded.index.PostingsEnum;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.Fields;
import org.apache.lucene5_shaded.index.MergeState;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.search.DocIdSetIterator;
import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.BytesRefBuilder;

/**
 * Codec API for writing term vectors:
 * <ol>
 *   <li>For every document, {@link #startDocument(int)} is called,
 *       informing the Codec how many fields will be written.
 *   <li>{@link #startField(FieldInfo, int, boolean, boolean, boolean)} is called for 
 *       each field in the document, informing the codec how many terms
 *       will be written for that field, and whether or not positions,
 *       offsets, or payloads are enabled.
 *   <li>Within each field, {@link #startTerm(BytesRef, int)} is called
 *       for each term.
 *   <li>If offsets and/or positions are enabled, then 
 *       {@link #addPosition(int, int, int, BytesRef)} will be called for each term
 *       occurrence.
 *   <li>After all documents have been written, {@link #finish(FieldInfos, int)} 
 *       is called for verification/sanity-checks.
 *   <li>Finally the writer is closed ({@link #close()})
 * </ol>
 * 
 * @lucene.experimental
 */
public abstract class TermVectorsWriter implements Closeable {
  
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected TermVectorsWriter() {
  }

  /** Called before writing the term vectors of the document.
   *  {@link #startField(FieldInfo, int, boolean, boolean, boolean)} will 
   *  be called <code>numVectorFields</code> times. Note that if term 
   *  vectors are enabled, this is called even if the document 
   *  has no vector fields, in this case <code>numVectorFields</code> 
   *  will be zero. */
  public abstract void startDocument(int numVectorFields) throws IOException;

  /** Called after a doc and all its fields have been added. */
  public void finishDocument() throws IOException {};

  /** Called before writing the terms of the field.
   *  {@link #startTerm(BytesRef, int)} will be called <code>numTerms</code> times. */
  public abstract void startField(FieldInfo info, int numTerms, boolean positions, boolean offsets, boolean payloads) throws IOException;

  /** Called after a field and all its terms have been added. */
  public void finishField() throws IOException {};

  /** Adds a term and its term frequency <code>freq</code>.
   * If this field has positions and/or offsets enabled, then
   * {@link #addPosition(int, int, int, BytesRef)} will be called 
   * <code>freq</code> times respectively.
   */
  public abstract void startTerm(BytesRef term, int freq) throws IOException;

  /** Called after a term and all its positions have been added. */
  public void finishTerm() throws IOException {}

  /** Adds a term position and offsets */
  public abstract void addPosition(int position, int startOffset, int endOffset, BytesRef payload) throws IOException;

  /** Called before {@link #close()}, passing in the number
   *  of documents that were written. Note that this is 
   *  intentionally redundant (equivalent to the number of
   *  calls to {@link #startDocument(int)}, but a Codec should
   *  check that this is the case to detect the JRE bug described 
   *  in LUCENE-1282. */
  public abstract void finish(FieldInfos fis, int numDocs) throws IOException;

  /** 
   * Called by IndexWriter when writing new segments.
   * <p>
   * This is an expert API that allows the codec to consume 
   * positions and offsets directly from the indexer.
   * <p>
   * The default implementation calls {@link #addPosition(int, int, int, BytesRef)},
   * but subclasses can override this if they want to efficiently write 
   * all the positions, then all the offsets, for example.
   * <p>
   * NOTE: This API is extremely expert and subject to change or removal!!!
   * @lucene.internal
   */
  // TODO: we should probably nuke this and make a more efficient 4.x format
  // PreFlex-RW could then be slow and buffer (it's only used in tests...)
  public void addProx(int numProx, DataInput positions, DataInput offsets) throws IOException {
    int position = 0;
    int lastOffset = 0;
    BytesRefBuilder payload = null;

    for (int i = 0; i < numProx; i++) {
      final int startOffset;
      final int endOffset;
      final BytesRef thisPayload;
      
      if (positions == null) {
        position = -1;
        thisPayload = null;
      } else {
        int code = positions.readVInt();
        position += code >>> 1;
        if ((code & 1) != 0) {
          // This position has a payload
          final int payloadLength = positions.readVInt();

          if (payload == null) {
            payload = new BytesRefBuilder();
          }
          payload.grow(payloadLength);

          positions.readBytes(payload.bytes(), 0, payloadLength);
          payload.setLength(payloadLength);
          thisPayload = payload.get();
        } else {
          thisPayload = null;
        }
      }
      
      if (offsets == null) {
        startOffset = endOffset = -1;
      } else {
        startOffset = lastOffset + offsets.readVInt();
        endOffset = startOffset + offsets.readVInt();
        lastOffset = endOffset;
      }
      addPosition(position, startOffset, endOffset, thisPayload);
    }
  }
  
  /** Merges in the term vectors from the readers in 
   *  <code>mergeState</code>. The default implementation skips
   *  over deleted documents, and uses {@link #startDocument(int)},
   *  {@link #startField(FieldInfo, int, boolean, boolean, boolean)}, 
   *  {@link #startTerm(BytesRef, int)}, {@link #addPosition(int, int, int, BytesRef)},
   *  and {@link #finish(FieldInfos, int)},
   *  returning the number of documents that were written.
   *  Implementations can override this method for more sophisticated
   *  merging (bulk-byte copying, etc). */
  public int merge(MergeState mergeState) throws IOException {
    int docCount = 0;
    int numReaders = mergeState.maxDocs.length;
    for (int i = 0; i < numReaders; i++) {
      int maxDoc = mergeState.maxDocs[i];
      Bits liveDocs = mergeState.liveDocs[i];
      TermVectorsReader termVectorsReader = mergeState.termVectorsReaders[i];
      if (termVectorsReader != null) {
        termVectorsReader.checkIntegrity();
      }

      for (int docID=0;docID<maxDoc;docID++) {
        if (liveDocs != null && !liveDocs.get(docID)) {
          // skip deleted docs
          continue;
        }
        // NOTE: it's very important to first assign to vectors then pass it to
        // termVectorsWriter.addAllDocVectors; see LUCENE-1282
        Fields vectors;
        if (termVectorsReader == null) {
          vectors = null;
        } else {
          vectors = termVectorsReader.get(docID);
        }
        addAllDocVectors(vectors, mergeState);
        docCount++;
      }
    }
    finish(mergeState.mergeFieldInfos, docCount);
    return docCount;
  }
  
  /** Safe (but, slowish) default method to write every
   *  vector field in the document. */
  protected final void addAllDocVectors(Fields vectors, MergeState mergeState) throws IOException {
    if (vectors == null) {
      startDocument(0);
      finishDocument();
      return;
    }

    int numFields = vectors.size();
    if (numFields == -1) {
      // count manually! TODO: Maybe enforce that Fields.size() returns something valid?
      numFields = 0;
      for (final Iterator<String> it = vectors.iterator(); it.hasNext(); ) {
        it.next();
        numFields++;
      }
    }
    startDocument(numFields);
    
    String lastFieldName = null;
    
    TermsEnum termsEnum = null;
    PostingsEnum docsAndPositionsEnum = null;
    
    int fieldCount = 0;
    for(String fieldName : vectors) {
      fieldCount++;
      final FieldInfo fieldInfo = mergeState.mergeFieldInfos.fieldInfo(fieldName);

      assert lastFieldName == null || fieldName.compareTo(lastFieldName) > 0: "lastFieldName=" + lastFieldName + " fieldName=" + fieldName;
      lastFieldName = fieldName;

      final Terms terms = vectors.terms(fieldName);
      if (terms == null) {
        // FieldsEnum shouldn't lie...
        continue;
      }
      
      final boolean hasPositions = terms.hasPositions();
      final boolean hasOffsets = terms.hasOffsets();
      final boolean hasPayloads = terms.hasPayloads();
      assert !hasPayloads || hasPositions;
      
      int numTerms = (int) terms.size();
      if (numTerms == -1) {
        // count manually. It is stupid, but needed, as Terms.size() is not a mandatory statistics function
        numTerms = 0;
        termsEnum = terms.iterator();
        while(termsEnum.next() != null) {
          numTerms++;
        }
      }
      
      startField(fieldInfo, numTerms, hasPositions, hasOffsets, hasPayloads);
      termsEnum = terms.iterator();

      int termCount = 0;
      while(termsEnum.next() != null) {
        termCount++;

        final int freq = (int) termsEnum.totalTermFreq();
        
        startTerm(termsEnum.term(), freq);

        if (hasPositions || hasOffsets) {
          docsAndPositionsEnum = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.OFFSETS | PostingsEnum.PAYLOADS);
          assert docsAndPositionsEnum != null;
          
          final int docID = docsAndPositionsEnum.nextDoc();
          assert docID != DocIdSetIterator.NO_MORE_DOCS;
          assert docsAndPositionsEnum.freq() == freq;

          for(int posUpto=0; posUpto<freq; posUpto++) {
            final int pos = docsAndPositionsEnum.nextPosition();
            final int startOffset = docsAndPositionsEnum.startOffset();
            final int endOffset = docsAndPositionsEnum.endOffset();
            
            final BytesRef payload = docsAndPositionsEnum.getPayload();

            assert !hasPositions || pos >= 0 ;
            addPosition(pos, startOffset, endOffset, payload);
          }
        }
        finishTerm();
      }
      assert termCount == numTerms;
      finishField();
    }
    assert fieldCount == numFields;
    finishDocument();
  }

  @Override
  public abstract void close() throws IOException;
}
