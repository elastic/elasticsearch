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

import static org.apache.lucene5_shaded.index.FilterLeafReader.FilterFields;
import static org.apache.lucene5_shaded.index.FilterLeafReader.FilterTerms;
import static org.apache.lucene5_shaded.index.FilterLeafReader.FilterTermsEnum;

/** A {@link Fields} implementation that merges multiple
 *  Fields into one, and maps around deleted documents.
 *  This is used for merging. 
 *  @lucene.internal
 */
public class MappedMultiFields extends FilterFields {
  final MergeState mergeState;

  /** Create a new MappedMultiFields for merging, based on the supplied
   * mergestate and merged view of terms. */
  public MappedMultiFields(MergeState mergeState, MultiFields multiFields) {
    super(multiFields);
    this.mergeState = mergeState;
  }

  @Override
  public Terms terms(String field) throws IOException {
    MultiTerms terms = (MultiTerms) in.terms(field);
    if (terms == null) {
      return null;
    } else {
      return new MappedMultiTerms(field, mergeState, terms);
    }
  }

  private static class MappedMultiTerms extends FilterTerms {
    final MergeState mergeState;
    final String field;

    public MappedMultiTerms(String field, MergeState mergeState, MultiTerms multiTerms) {
      super(multiTerms);
      this.field = field;
      this.mergeState = mergeState;
    }

    @Override
    public TermsEnum iterator() throws IOException {
      TermsEnum iterator = in.iterator();
      if (iterator == TermsEnum.EMPTY) {
        // LUCENE-6826
        return TermsEnum.EMPTY;
      } else {
        return new MappedMultiTermsEnum(field, mergeState, (MultiTermsEnum) iterator);
      }
    }

    @Override
    public long size() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumDocFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  private static class MappedMultiTermsEnum extends FilterTermsEnum {
    final MergeState mergeState;
    final String field;

    public MappedMultiTermsEnum(String field, MergeState mergeState, MultiTermsEnum multiTermsEnum) {
      super(multiTermsEnum);
      this.field = field;
      this.mergeState = mergeState;
    }

    @Override
    public int docFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      MappingMultiPostingsEnum mappingDocsAndPositionsEnum;
      if (reuse instanceof MappingMultiPostingsEnum) {
        MappingMultiPostingsEnum postings = (MappingMultiPostingsEnum) reuse;
        if (postings.field.equals(this.field)) {
          mappingDocsAndPositionsEnum = postings;
        } else {
          mappingDocsAndPositionsEnum = new MappingMultiPostingsEnum(field, mergeState);
        }
      } else {
        mappingDocsAndPositionsEnum = new MappingMultiPostingsEnum(field, mergeState);
      }

      MultiPostingsEnum docsAndPositionsEnum = (MultiPostingsEnum) in.postings(mappingDocsAndPositionsEnum.multiDocsAndPositionsEnum, flags);
      mappingDocsAndPositionsEnum.reset(docsAndPositionsEnum);
      return mappingDocsAndPositionsEnum;
    }
  }
}
