package org.apache.lucene.search.suggest.xdocument;

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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.*;

import static org.apache.lucene.search.suggest.xdocument.CompletionPostingsFormat.*;

/**
 * <p>
 * Completion index (.cmp) is opened and read at instantiation to read in {@link SuggestField}
 * numbers and their FST offsets in the Completion dictionary (.lkp).
 * </p>
 * <p>
 * Completion dictionary (.lkp) is opened at instantiation and a field's FST is loaded
 * into memory the first time it is requested via {@link #terms(String)}.
 * </p>
 * <p>
 * NOTE: Only the footer is validated for Completion dictionary (.lkp) and not the checksum due
 * to random access pattern and checksum validation being too costly at instantiation
 * </p>
 *
 */
final class CompletionFieldsProducer extends FieldsProducer {

  private FieldsProducer delegateFieldsProducer;
  private Map<String, CompletionsTermsReader> readers;
  private IndexInput dictIn;

  // copy ctr for merge instance
  private CompletionFieldsProducer(FieldsProducer delegateFieldsProducer, Map<String, CompletionsTermsReader> readers) {
    this.delegateFieldsProducer = delegateFieldsProducer;
    this.readers = readers;
  }

  CompletionFieldsProducer(SegmentReadState state) throws IOException {
    String indexFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, INDEX_EXTENSION);
    delegateFieldsProducer = null;
    boolean success = false;

    try (ChecksumIndexInput index = state.directory.openChecksumInput(indexFile, state.context)) {
      // open up dict file containing all fsts
      String dictFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, DICT_EXTENSION);
      dictIn = state.directory.openInput(dictFile, state.context);
      CodecUtil.checkIndexHeader(dictIn, CODEC_NAME, COMPLETION_CODEC_VERSION, COMPLETION_VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      // just validate the footer for the dictIn
      CodecUtil.retrieveChecksum(dictIn);

      // open up index file (fieldNumber, offset)
      CodecUtil.checkIndexHeader(index, CODEC_NAME, COMPLETION_CODEC_VERSION, COMPLETION_VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      // load delegate PF
      PostingsFormat delegatePostingsFormat = PostingsFormat.forName(index.readString());
      delegateFieldsProducer = delegatePostingsFormat.fieldsProducer(state);

      // read suggest field numbers and their offsets in the terms file from index
      int numFields = index.readVInt();
      readers = new HashMap<>(numFields);
      for (int i = 0; i < numFields; i++) {
        int fieldNumber = index.readVInt();
        long offset = index.readVLong();
        long minWeight = index.readVLong();
        long maxWeight = index.readVLong();
        byte type = index.readByte();
        FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNumber);
        // we don't load the FST yet
        readers.put(fieldInfo.name, new CompletionsTermsReader(dictIn, offset, minWeight, maxWeight, type));
      }
      CodecUtil.checkFooter(index);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(delegateFieldsProducer, dictIn);
      }
    }
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      delegateFieldsProducer.close();
      IOUtils.close(dictIn);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(delegateFieldsProducer, dictIn);
      }
    }
  }

  @Override
  public void checkIntegrity() throws IOException {
    delegateFieldsProducer.checkIntegrity();
    // TODO: checkIntegrity should checksum the dictionary and index
  }

  @Override
  public FieldsProducer getMergeInstance() throws IOException {
    return new CompletionFieldsProducer(delegateFieldsProducer, readers);
  }

  @Override
  public long ramBytesUsed() {
    long ramBytesUsed = delegateFieldsProducer.ramBytesUsed();
    for (CompletionsTermsReader reader : readers.values()) {
      ramBytesUsed += reader.ramBytesUsed();
    }
    return ramBytesUsed;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> accountableList = new ArrayList<>();
    for (Map.Entry<String, CompletionsTermsReader> readerEntry : readers.entrySet()) {
      accountableList.add(Accountables.namedAccountable(readerEntry.getKey(), readerEntry.getValue()));
    }
    return Collections.unmodifiableCollection(accountableList);
  }

  @Override
  public Iterator<String> iterator() {
    return readers.keySet().iterator();
  }

  @Override
  public Terms terms(String field) throws IOException {
    Terms terms = delegateFieldsProducer.terms(field) ;
    if (terms == null) {
      return null;
    }
    return new CompletionTerms(terms, readers.get(field));
  }

  @Override
  public int size() {
    return readers.size();
  }

}
