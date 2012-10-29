/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.search;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Similar to {@link TermsFilter} but stores the terms in an array for better memory usage
 * when cached, and also uses bulk read
 */
// LUCENE MONITOR: Against TermsFilter - this is now identical to TermsFilter once 4.1 is released
public class XTermsFilter extends Filter {

    private final Term[] filterTerms;
    private final boolean[] resetTermsEnum;// true if the enum must be reset when building the bitset
    private final int length;
    
    /**
     * Creates a new {@link XTermsFilter} from the given collection. The collection
     * can contain duplicate terms and multiple fields.
     */
    public XTermsFilter(Collection<Term> terms) {
      this(terms.toArray(new Term[terms.size()]));
    }
    
    /**
     * Creates a new {@link XTermsFilter} from the given array. The array can
     * contain duplicate terms and multiple fields.
     */
    public XTermsFilter(Term... terms) {
      if (terms == null || terms.length == 0) {
        throw new IllegalArgumentException("TermsFilter requires at least one term");
      }
      Arrays.sort(terms);
      this.filterTerms = new Term[terms.length];
      this.resetTermsEnum = new boolean[terms.length];
      int index = 0;
      for (int i = 0; i < terms.length; i++) {
        Term currentTerm = terms[i];
        boolean fieldChanged = true;
        if (index > 0) {
          // deduplicate
          if (filterTerms[index-1].field().equals(currentTerm.field())) {
            fieldChanged = false;
            if (filterTerms[index-1].bytes().bytesEquals(currentTerm.bytes())){
              continue;            
            }
          }
        }
        this.filterTerms[index] = currentTerm;
        this.resetTermsEnum[index] = index == 0 || fieldChanged; // mark index 0 so we have a clear path in the iteration
        
        index++;
      }
      length = index;
    }

    
    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      AtomicReader reader = context.reader();
      FixedBitSet result = null;  // lazy init if needed - no need to create a big bitset ahead of time
      Fields fields = reader.fields();
      if (fields == null) {
        return result;
      }
      final BytesRef br = new BytesRef();
      Terms terms = null;
      TermsEnum termsEnum = null;
      DocsEnum docs = null;
      assert resetTermsEnum[0];
      for (int i = 0; i < length; i++) {
        Term term = this.filterTerms[i];
        if (resetTermsEnum[i]) {
          terms = fields.terms(term.field());
          if (terms == null) {
            i = skipToNextField(i+1, length); // skip to the next field since this field is not indexed
            continue;
          }
        }
        if ((termsEnum = terms.iterator(termsEnum)) != null) {
          br.copyBytes(term.bytes());
          assert termsEnum != null;
          if (termsEnum.seekExact(br,true)) {
            docs = termsEnum.docs(acceptDocs, docs, 0);
            if (result == null) {
              if (docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                result = new FixedBitSet(reader.maxDoc());
                // lazy init but don't do it in the hot loop since we could read many docs
                result.set(docs.docID());
              }
            }
            while (docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              result.set(docs.docID());
            }
          }
        }
      }
      return result;
    }

    private final int skipToNextField(int index, int length) {
      for (int i = index; i < length; i++) {
        if (resetTermsEnum[i]) {
          return i-1;
        }
      }
      return length;
    }
      

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if ((obj == null) || (obj.getClass() != this.getClass())) {
        return false;
      }
      XTermsFilter test = (XTermsFilter) obj;
      if (filterTerms != test.filterTerms) {
        if (length == test.length) {
          for (int i = 0; i < length; i++) {
            // can not be null!
            if (!filterTerms[i].equals(test.filterTerms[i])) {
              return false;
            }
          }
        } else {
          return false;
        }
      }
      return true;
      
    }

    @Override
    public int hashCode() {
      int hash = 9;
      for (int i = 0; i < length; i++) {
        hash = 31 * hash + filterTerms[i].hashCode();
      }
      return hash;
    }
    
    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < length; i++) {
        if (builder.length() > 0) {
          builder.append(' ');
        }
        builder.append(filterTerms[i]);
      }
      return builder.toString();
    }
}
