/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

/**
 * Comparator source for string/binary values.
 */
public class BytesRefFieldComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexFieldData<?> indexFieldData;
    private final MultiValueMode sortMode;
    private final Object missingValue;
    private final Nested nested;

    public BytesRefFieldComparatorSource(IndexFieldData<?> indexFieldData, Object missingValue, MultiValueMode sortMode, Nested nested) {
        this.indexFieldData = indexFieldData;
        this.sortMode = sortMode;
        this.missingValue = missingValue;
        this.nested = nested;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.STRING;
    }

    protected SortedBinaryDocValues getValues(AtomicReaderContext context) {
        return indexFieldData.load(context).getBytesValues();
    }

    protected void setScorer(Scorer scorer) {}

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldNames().indexName());

        final boolean sortMissingLast = sortMissingLast(missingValue) ^ reversed;
        final BytesRef missingBytes = (BytesRef) missingObject(missingValue, reversed);
        if (indexFieldData instanceof IndexOrdinalsFieldData) {
            return new FieldComparator.TermOrdValComparator(numHits, null, sortMissingLast) {
                
                @Override
                protected SortedDocValues getSortedDocValues(AtomicReaderContext context, String field) throws IOException {
                    final RandomAccessOrds values = ((IndexOrdinalsFieldData) indexFieldData).load(context).getOrdinalsValues();
                    final SortedDocValues selectedValues;
                    if (nested == null) {
                        selectedValues = sortMode.select(values);
                    } else {
                        final FixedBitSet rootDocs = nested.rootDocs(context);
                        final FixedBitSet innerDocs = nested.innerDocs(context);
                        selectedValues = sortMode.select(values, rootDocs, innerDocs);
                    }
                    if (sortMissingFirst(missingValue) || sortMissingLast(missingValue)) {
                        return selectedValues;
                    } else {
                        return new ReplaceMissing(selectedValues, missingBytes);
                    }
                }
                
                public BytesRef value(int slot) {
                    // TODO: When serializing the response to the coordinating node, we lose the information about
                    // whether the comparator sorts missing docs first or last. We should fix it and let
                    // TopDocs.merge deal with it (it knows how to)
                    BytesRef value = super.value(slot);
                    if (value == null) {
                        value = missingBytes;
                    }
                    return value;
                }
                
            };
        }

        final BytesRef nullPlaceHolder = new BytesRef();
        final BytesRef nonNullMissingBytes = missingBytes == null ? nullPlaceHolder : missingBytes;
        return new TermValComparator(numHits, null, sortMissingLast) {

            @Override
            protected BinaryDocValues getBinaryDocValues(AtomicReaderContext context, String field) throws IOException {
                final SortedBinaryDocValues values = getValues(context);
                final BinaryDocValues selectedValues;
                if (nested == null) {
                    selectedValues = sortMode.select(values, nonNullMissingBytes);
                } else {
                    final FixedBitSet rootDocs = nested.rootDocs(context);
                    final FixedBitSet innerDocs = nested.innerDocs(context);
                    selectedValues = sortMode.select(values, nonNullMissingBytes, rootDocs, innerDocs, context.reader().maxDoc());
                }
                return selectedValues;
            }

            @Override
            protected Bits getDocsWithField(AtomicReaderContext context, String field) throws IOException {
                return new Bits.MatchAllBits(context.reader().maxDoc());
            }

            @Override
            protected boolean isNull(int doc, BytesRef term) {
                return term == nullPlaceHolder;
            }

            @Override
            public void setScorer(Scorer scorer) {
                BytesRefFieldComparatorSource.this.setScorer(scorer);
            }

            @Override
            public BytesRef value(int slot) {
                BytesRef value = super.value(slot);
                if (value == null) {
                    value = missingBytes;
                }
                return value;
            }

        };
    }
    
    /** 
     * A view of a SortedDocValues where missing values 
     * are replaced with the specified term  
     */
    // TODO: move this out if we need it for other reasons
    static class ReplaceMissing extends SortedDocValues {
        final SortedDocValues in;
        final int substituteOrd;
        final BytesRef substituteTerm;
        final boolean exists;
        
        ReplaceMissing(SortedDocValues in, BytesRef term) {
            this.in = in;
            this.substituteTerm = term;
            int sub = in.lookupTerm(term);
            if (sub < 0) {
                substituteOrd = -sub-1;
                exists = false;
            } else {
                substituteOrd = sub;
                exists = true;
            }
        }

        @Override
        public int getOrd(int docID) {
            int ord = in.getOrd(docID);
            if (ord < 0) {
                return substituteOrd;
            } else if (exists == false && ord >= substituteOrd) {
                return ord + 1;
            } else {
                return ord;
            }
        }

        @Override
        public int getValueCount() {
            if (exists) {
                return in.getValueCount();
            } else {
                return in.getValueCount() + 1;
            }
        }

        @Override
        public BytesRef lookupOrd(int ord) {
            if (ord == substituteOrd) {
                return substituteTerm;
            } else if (exists == false && ord > substituteOrd) {
                return in.lookupOrd(ord-1);
            } else {
                return in.lookupOrd(ord);
            }
        }
        
        // we let termsenum etc fall back to the default implementation
    }

    static {
        assert Lucene.VERSION == Version.LUCENE_4_9 : "The comparator below is a raw copy of Lucene's, remove it when upgrading to 4.10";
    }

    /** Sorts by field's natural Term sort order.  All
     *  comparisons are done using BytesRef.compareTo, which is
     *  slow for medium to large result sets but possibly
     *  very fast for very small results sets. */
    public static class TermValComparator extends FieldComparator<BytesRef> {

      private final BytesRef[] values;
      private final BytesRef[] tempBRs;
      private BinaryDocValues docTerms;
      private Bits docsWithField;
      private final String field;
      private BytesRef bottom;
      private BytesRef topValue;
      private final int missingSortCmp;

      /** Sole constructor. */
      public TermValComparator(int numHits, String field, boolean sortMissingLast) {
        values = new BytesRef[numHits];
        tempBRs = new BytesRef[numHits];
        this.field = field;
        missingSortCmp = sortMissingLast ? 1 : -1;
      }

      @Override
      public int compare(int slot1, int slot2) {
        final BytesRef val1 = values[slot1];
        final BytesRef val2 = values[slot2];
        return compareValues(val1, val2);
      }

      @Override
      public int compareBottom(int doc) {
        final BytesRef comparableBytes = getComparableBytes(doc, docTerms.get(doc));
        return compareValues(bottom, comparableBytes);
      }

      @Override
      public void copy(int slot, int doc) {
        final BytesRef comparableBytes = getComparableBytes(doc, docTerms.get(doc));
        if (comparableBytes == null) {
          values[slot] = null;
        } else {
          if (tempBRs[slot] == null) {
            tempBRs[slot] = new BytesRef();
          }
          values[slot] = tempBRs[slot];
          values[slot].copyBytes(comparableBytes);
        }
      }

      /** Retrieves the BinaryDocValues for the field in this segment */
      protected BinaryDocValues getBinaryDocValues(AtomicReaderContext context, String field) throws IOException {
        return FieldCache.DEFAULT.getTerms(context.reader(), field, true);
      }

      /** Retrieves the set of documents that have a value in this segment */
      protected Bits getDocsWithField(AtomicReaderContext context, String field) throws IOException {
        return FieldCache.DEFAULT.getDocsWithField(context.reader(), field);
      }

      /** Check whether the given value represents <tt>null</tt>. This can be
       *  useful if the {@link BinaryDocValues} returned by {@link #getBinaryDocValues}
       *  use a special value as a sentinel. The default implementation checks
       *  {@link #getDocsWithField}.
       *  <p>NOTE: The null value can only be an EMPTY {@link BytesRef}. */
      protected boolean isNull(int doc, BytesRef term) {
        return docsWithField != null && docsWithField.get(doc) == false;
      }

      @Override
      public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        docTerms = getBinaryDocValues(context, field);
        docsWithField = getDocsWithField(context, field);
        if (docsWithField instanceof Bits.MatchAllBits) {
          docsWithField = null;
        }
        return this;
      }

      @Override
      public void setBottom(final int bottom) {
        this.bottom = values[bottom];
      }

      @Override
      public void setTopValue(BytesRef value) {
        // null is fine: it means the last doc of the prior
        // search was missing this value
        topValue = value;
      }

      @Override
      public BytesRef value(int slot) {
        return values[slot];
      }

      @Override
      public int compareValues(BytesRef val1, BytesRef val2) {
        // missing always sorts first:
        if (val1 == null) {
          if (val2 == null) {
            return 0;
          }
          return missingSortCmp;
        } else if (val2 == null) {
          return -missingSortCmp;
        }
        return val1.compareTo(val2);
      }

      @Override
      public int compareTop(int doc) {
        final BytesRef comparableBytes = getComparableBytes(doc, docTerms.get(doc));
        return compareValues(topValue, comparableBytes);
      }

      /**
       * Given a document and a term, return the term itself if it exists or
       * <tt>null</tt> otherwise.
       */
      private BytesRef getComparableBytes(int doc, BytesRef term) {
        if (term.length == 0 && isNull(doc, term)) {
          return null;
        }
        return term;
      }
    }

}
