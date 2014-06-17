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
package org.elasticsearch.index.search.nested;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.NestedWrappableComparator;
import org.elasticsearch.index.fielddata.fieldcomparator.NumberComparatorBase;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Locale;

/**
 */
public class NestedFieldComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final MultiValueMode sortMode;
    private final IndexFieldData.XFieldComparatorSource wrappedSource;
    private final Filter rootDocumentsFilter;
    private final Filter innerDocumentsFilter;

    public NestedFieldComparatorSource(MultiValueMode sortMode, IndexFieldData.XFieldComparatorSource wrappedSource, Filter rootDocumentsFilter, Filter innerDocumentsFilter) {
        this.sortMode = sortMode;
        this.wrappedSource = wrappedSource;
        this.rootDocumentsFilter = rootDocumentsFilter;
        this.innerDocumentsFilter = innerDocumentsFilter;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        // +1: have one spare slot for value comparison between inner documents.
        FieldComparator wrappedComparator = wrappedSource.newComparator(fieldname, numHits + 1, sortPos, reversed);
        switch (sortMode) {
            case MAX:
                return new NestedFieldComparator.Highest(wrappedComparator, rootDocumentsFilter, innerDocumentsFilter, numHits);
            case MIN:
                return new NestedFieldComparator.Lowest(wrappedComparator, rootDocumentsFilter, innerDocumentsFilter, numHits);
            case SUM:
                return new NestedFieldComparator.Sum((NumberComparatorBase<?>) wrappedComparator, rootDocumentsFilter, innerDocumentsFilter, numHits);
            case AVG:
                return new NestedFieldComparator.Avg((NumberComparatorBase<?>) wrappedComparator, rootDocumentsFilter, innerDocumentsFilter, numHits);
            default:
                throw new ElasticsearchIllegalArgumentException(
                    String.format(Locale.ROOT, "Unsupported sort_mode[%s] for nested type", sortMode)
                );
        }
    }

    @Override
    public SortField.Type reducedType() {
        return wrappedSource.reducedType();
    }

}

abstract class NestedFieldComparator extends FieldComparator {

    final Filter rootDocumentsFilter;
    final Filter innerDocumentsFilter;
    final int spareSlot;

    FieldComparator wrappedComparator;
    FixedBitSet rootDocuments;
    FixedBitSet innerDocuments;
    int bottomSlot;
    Object top;

    NestedFieldComparator(FieldComparator wrappedComparator, Filter rootDocumentsFilter, Filter innerDocumentsFilter, int spareSlot) {
        this.wrappedComparator = wrappedComparator;
        this.rootDocumentsFilter = rootDocumentsFilter;
        this.innerDocumentsFilter = innerDocumentsFilter;
        this.spareSlot = spareSlot;
    }

    @Override
    public final int compare(int slot1, int slot2) {
        return wrappedComparator.compare(slot1, slot2);
    }

    @Override
    public final void setBottom(int slot) {
        wrappedComparator.setBottom(slot);
        this.bottomSlot = slot;
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
        DocIdSet innerDocuments = innerDocumentsFilter.getDocIdSet(context, null);
        if (DocIdSets.isEmpty(innerDocuments)) {
            this.innerDocuments = null;
        } else if (innerDocuments instanceof FixedBitSet) {
            this.innerDocuments = (FixedBitSet) innerDocuments;
        } else {
            this.innerDocuments = DocIdSets.toFixedBitSet(innerDocuments.iterator(), context.reader().maxDoc());
        }
        DocIdSet rootDocuments = rootDocumentsFilter.getDocIdSet(context, null);
        if (DocIdSets.isEmpty(rootDocuments)) {
            this.rootDocuments = null;
        } else if (rootDocuments instanceof FixedBitSet) {
            this.rootDocuments = (FixedBitSet) rootDocuments;
        } else {
            this.rootDocuments = DocIdSets.toFixedBitSet(rootDocuments.iterator(), context.reader().maxDoc());
        }

        wrappedComparator = wrappedComparator.setNextReader(context);
        return this;
    }

    @Override
    public final Object value(int slot) {
        return wrappedComparator.value(slot);
    }

    @Override
    public void setTopValue(Object top) {
        this.top = top;
        wrappedComparator.setTopValue(top);
    }

    final static class Lowest extends NestedFieldComparator {

        Lowest(FieldComparator wrappedComparator, Filter parentFilter, Filter childFilter, int spareSlot) {
            super(wrappedComparator, parentFilter, childFilter, spareSlot);
        }

        @Override
        public int compareBottom(int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null || innerDocuments == null) {
                return compareBottomMissing(wrappedComparator);
            }

            // We need to copy the lowest value from all nested docs into slot.
            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            int nestedDoc = innerDocuments.nextSetBit(prevRootDoc + 1);
            if (nestedDoc >= rootDoc || nestedDoc == -1) {
                return compareBottomMissing(wrappedComparator);
            }

            // We only need to emit a single cmp value for any matching nested doc
            int cmp = wrappedComparator.compareBottom(nestedDoc);
            if (cmp > 0) {
                return cmp;
            }

            while (true) {
                nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
                if (nestedDoc >= rootDoc || nestedDoc == -1) {
                    return cmp;
                }
                int cmp1 = wrappedComparator.compareBottom(nestedDoc);
                if (cmp1 > 0) {
                    return cmp1;
                } else {
                    if (cmp1 == 0) {
                        cmp = 0;
                    }
                }
            }
        }

        @Override
        public void copy(int slot, int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null || innerDocuments == null) {
                copyMissing(wrappedComparator, slot);
                return;
            }

            // We need to copy the lowest value from all nested docs into slot.
            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            int nestedDoc = innerDocuments.nextSetBit(prevRootDoc + 1);
            if (nestedDoc >= rootDoc || nestedDoc == -1) {
                copyMissing(wrappedComparator, slot);
                return;
            }
            wrappedComparator.copy(slot, nestedDoc);

            while (true) {
                nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
                if (nestedDoc >= rootDoc || nestedDoc == -1) {
                    return;
                }
                wrappedComparator.copy(spareSlot, nestedDoc);
                if (wrappedComparator.compare(spareSlot, slot) < 0) {
                    wrappedComparator.copy(slot, nestedDoc);
                }
            }
        }

        @Override
        public int compareTop(int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null || innerDocuments == null) {
                return compareTopMissing(wrappedComparator);
            }

            // We need to copy the lowest value from all nested docs into slot.
            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            int nestedDoc = innerDocuments.nextSetBit(prevRootDoc + 1);
            if (nestedDoc >= rootDoc || nestedDoc == -1) {
                return compareTopMissing(wrappedComparator);
            }

            // We only need to emit a single cmp value for any matching nested doc
            @SuppressWarnings("unchecked")
            int cmp = wrappedComparator.compareTop(nestedDoc);
            if (cmp > 0) {
                return cmp;
            }

            while (true) {
                nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
                if (nestedDoc >= rootDoc || nestedDoc == -1) {
                    return cmp;
                }
                @SuppressWarnings("unchecked")
                int cmp1 = wrappedComparator.compareTop(nestedDoc);
                if (cmp1 > 0) {
                    return cmp1;
                } else {
                    if (cmp1 == 0) {
                        cmp = 0;
                    }
                }
            }
        }
    }

    final static class Highest extends NestedFieldComparator {

        Highest(FieldComparator wrappedComparator, Filter parentFilter, Filter childFilter, int spareSlot) {
            super(wrappedComparator, parentFilter, childFilter, spareSlot);
        }

        @Override
        public int compareBottom(int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null || innerDocuments == null) {
                return compareBottomMissing(wrappedComparator);
            }

            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            int nestedDoc = innerDocuments.nextSetBit(prevRootDoc + 1);
            if (nestedDoc >= rootDoc || nestedDoc == -1) {
                return compareBottomMissing(wrappedComparator);
            }

            int cmp = wrappedComparator.compareBottom(nestedDoc);
            if (cmp < 0) {
                return cmp;
            }

            while (true) {
                nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
                if (nestedDoc >= rootDoc || nestedDoc == -1) {
                    return cmp;
                }
                int cmp1 = wrappedComparator.compareBottom(nestedDoc);
                if (cmp1 < 0) {
                    return cmp1;
                } else if (cmp1 == 0) {
                   cmp = 0;
                }
            }
        }

        @Override
        public void copy(int slot, int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null || innerDocuments == null) {
                copyMissing(wrappedComparator, slot);
                return;
            }

            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            int nestedDoc = innerDocuments.nextSetBit(prevRootDoc + 1);
            if (nestedDoc >= rootDoc || nestedDoc == -1) {
                copyMissing(wrappedComparator, slot);
                return;
            }
            wrappedComparator.copy(slot, nestedDoc);

            while (true) {
                nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
                if (nestedDoc >= rootDoc || nestedDoc == -1) {
                    return;
                }
                wrappedComparator.copy(spareSlot, nestedDoc);
                if (wrappedComparator.compare(spareSlot, slot) > 0) {
                    wrappedComparator.copy(slot, nestedDoc);
                }
            }
        }

        @Override
        public int compareTop(int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null || innerDocuments == null) {
                return compareTopMissing(wrappedComparator);
            }

            int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            int nestedDoc = innerDocuments.nextSetBit(prevRootDoc + 1);
            if (nestedDoc >= rootDoc || nestedDoc == -1) {
                return compareTopMissing(wrappedComparator);
            }

            @SuppressWarnings("unchecked")
            int cmp = wrappedComparator.compareTop(nestedDoc);
            if (cmp < 0) {
                return cmp;
            }

            while (true) {
                nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
                if (nestedDoc >= rootDoc || nestedDoc == -1) {
                    return cmp;
                }
                @SuppressWarnings("unchecked")
                int cmp1 = wrappedComparator.compareTop(nestedDoc);
                if (cmp1 < 0) {
                    return cmp1;
                } else if (cmp1 == 0) {
                    cmp = 0;
                }
            }
        }
    }
    
    static abstract class NumericNestedFieldComparatorBase extends NestedFieldComparator {
        protected NumberComparatorBase numberComparator;

        NumericNestedFieldComparatorBase(NumberComparatorBase wrappedComparator, Filter rootDocumentsFilter, Filter innerDocumentsFilter, int spareSlot) {
            super(wrappedComparator, rootDocumentsFilter, innerDocumentsFilter, spareSlot);
            this.numberComparator = wrappedComparator;
        }

        @Override
        public final int compareBottom(int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null || innerDocuments == null) {
                return compareBottomMissing(wrappedComparator);
            }

            final int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            int nestedDoc = innerDocuments.nextSetBit(prevRootDoc + 1);
            if (nestedDoc >= rootDoc || nestedDoc == -1) {
                return compareBottomMissing(wrappedComparator);
            }

            int counter = 1;
            wrappedComparator.copy(spareSlot, nestedDoc);
            nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
            while (nestedDoc > prevRootDoc && nestedDoc < rootDoc) {
                onNested(spareSlot, nestedDoc);
                nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
                counter++;
            }
            afterNested(spareSlot, counter);
            return compare(bottomSlot, spareSlot);
        }

        @Override
        public final void copy(int slot, int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null || innerDocuments == null) {
                copyMissing(wrappedComparator, slot);
                return;
            }

            final int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            int nestedDoc = innerDocuments.nextSetBit(prevRootDoc + 1);
            if (nestedDoc >= rootDoc || nestedDoc == -1) {
                copyMissing(wrappedComparator, slot);
                return;
            }
            int counter = 1;
            wrappedComparator.copy(slot, nestedDoc);
            nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
            while (nestedDoc > prevRootDoc && nestedDoc < rootDoc) {
                onNested(slot, nestedDoc);
                nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
                counter++;
            }
            afterNested(slot, counter);
        }

        @Override
        @SuppressWarnings("unchecked")
        public int compareTop(int rootDoc) throws IOException {
            if (rootDoc == 0 || rootDocuments == null || innerDocuments == null) {
                return compareTopMissing(wrappedComparator);
            }

            final int prevRootDoc = rootDocuments.prevSetBit(rootDoc - 1);
            int nestedDoc = innerDocuments.nextSetBit(prevRootDoc + 1);
            if (nestedDoc >= rootDoc || nestedDoc == -1) {
                return compareTopMissing(wrappedComparator);
            }

            int counter = 1;
            wrappedComparator.copy(spareSlot, nestedDoc);
            nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
            while (nestedDoc > prevRootDoc && nestedDoc < rootDoc) {
                onNested(spareSlot, nestedDoc);
                nestedDoc = innerDocuments.nextSetBit(nestedDoc + 1);
                counter++;
            }
            afterNested(spareSlot, counter);
            return wrappedComparator.compareValues(wrappedComparator.value(spareSlot), top);
        }

        protected abstract void onNested(int slot, int nestedDoc);
        
        protected abstract void afterNested(int slot, int count);

        @Override
        public final FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
            super.setNextReader(context);
            numberComparator = (NumberComparatorBase) super.wrappedComparator;
            return this;
        }
    }

    final static class Sum extends NumericNestedFieldComparatorBase {

        Sum(NumberComparatorBase wrappedComparator, Filter rootDocumentsFilter, Filter innerDocumentsFilter, int spareSlot) {
            super(wrappedComparator, rootDocumentsFilter, innerDocumentsFilter, spareSlot);
        }

        @Override
        protected void onNested(int slot, int nestedDoc) {
            numberComparator.add(slot, nestedDoc);
        }

        @Override
        protected void afterNested(int slot, int count) {
        }

    }

    final static class Avg extends NumericNestedFieldComparatorBase {
        Avg(NumberComparatorBase wrappedComparator, Filter rootDocumentsFilter, Filter innerDocumentsFilter, int spareSlot) {
            super(wrappedComparator, rootDocumentsFilter, innerDocumentsFilter, spareSlot);
        }

        @Override
        protected void onNested(int slot, int nestedDoc) {
            numberComparator.add(slot, nestedDoc);
        }

        @Override
        protected void afterNested(int slot, int count) {
            numberComparator.divide(slot, count);
        }

      
    }

    static final void copyMissing(FieldComparator<?> comparator, int slot) {
        if (comparator instanceof NestedWrappableComparator<?>) {
            ((NestedWrappableComparator<?>) comparator).missing(slot);
        }
    }

    static final int compareBottomMissing(FieldComparator<?> comparator) {
        if (comparator instanceof NestedWrappableComparator<?>) {
            return ((NestedWrappableComparator<?>) comparator).compareBottomMissing();
        } else {
            return 0;
        }
    }

    @SuppressWarnings("unchecked")
    static final int compareTopMissing(FieldComparator<?> comparator) {
        if (comparator instanceof NestedWrappableComparator) {
            return ((NestedWrappableComparator) comparator).compareTopMissing();
        } else {
            return 0;
        }
    }

}