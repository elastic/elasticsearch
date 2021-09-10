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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene5_shaded.codecs.FieldsConsumer;
import org.apache.lucene5_shaded.util.CollectionUtil;
import org.apache.lucene5_shaded.util.IOUtils;

final class FreqProxTermsWriter extends TermsHash {

  public FreqProxTermsWriter(DocumentsWriterPerThread docWriter, TermsHash termVectors) {
    super(docWriter, true, termVectors);
  }

  private void applyDeletes(SegmentWriteState state, Fields fields) throws IOException {

    // Process any pending Term deletes for this newly
    // flushed segment:
    if (state.segUpdates != null && state.segUpdates.terms.size() > 0) {
      Map<Term,Integer> segDeletes = state.segUpdates.terms;
      List<Term> deleteTerms = new ArrayList<>(segDeletes.keySet());
      Collections.sort(deleteTerms);
      String lastField = null;
      TermsEnum termsEnum = null;
      PostingsEnum postingsEnum = null;
      for(Term deleteTerm : deleteTerms) {
        if (deleteTerm.field().equals(lastField) == false) {
          lastField = deleteTerm.field();
          Terms terms = fields.terms(lastField);
          if (terms != null) {
            termsEnum = terms.iterator();
          } else {
            termsEnum = null;
          }
        }

        if (termsEnum != null && termsEnum.seekExact(deleteTerm.bytes())) {
          postingsEnum = termsEnum.postings(postingsEnum, 0);
          int delDocLimit = segDeletes.get(deleteTerm);
          assert delDocLimit < PostingsEnum.NO_MORE_DOCS;
          while (true) {
            int doc = postingsEnum.nextDoc();
            if (doc < delDocLimit) {
              if (state.liveDocs == null) {
                state.liveDocs = state.segmentInfo.getCodec().liveDocsFormat().newLiveDocs(state.segmentInfo.maxDoc());
              }
              if (state.liveDocs.get(doc)) {
                state.delCountOnFlush++;
                state.liveDocs.clear(doc);
              }
            } else {
              break;
            }
          }
        }
      }
    }
  }

  @Override
  public void flush(Map<String,TermsHashPerField> fieldsToFlush, final SegmentWriteState state) throws IOException {
    super.flush(fieldsToFlush, state);

    // Gather all fields that saw any postings:
    List<FreqProxTermsWriterPerField> allFields = new ArrayList<>();

    for (TermsHashPerField f : fieldsToFlush.values()) {
      final FreqProxTermsWriterPerField perField = (FreqProxTermsWriterPerField) f;
      if (perField.bytesHash.size() > 0) {
        perField.sortPostings();
        assert perField.fieldInfo.getIndexOptions() != IndexOptions.NONE;
        allFields.add(perField);
      }
    }

    // Sort by field name
    CollectionUtil.introSort(allFields);

    Fields fields = new FreqProxFields(allFields);

    applyDeletes(state, fields);

    FieldsConsumer consumer = state.segmentInfo.getCodec().postingsFormat().fieldsConsumer(state);
    boolean success = false;
    try {
      consumer.write(fields);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);
      }
    }

  }

  @Override
  public TermsHashPerField addField(FieldInvertState invertState, FieldInfo fieldInfo) {
    return new FreqProxTermsWriterPerField(invertState, this, fieldInfo, nextTermsHash.addField(invertState, fieldInfo));
  }
}
