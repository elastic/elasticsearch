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

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.fielddata.AbstractSortedDocValues;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class SinglePackedOrdinals extends Ordinals {

    // ordinals with value 0 indicates no value
    private final PackedInts.Reader reader;
    private final int valueCount;

    public SinglePackedOrdinals(OrdinalsBuilder builder, float acceptableOverheadRatio) {
        assert builder.getNumMultiValuesDocs() == 0;
        this.valueCount = (int) builder.getValueCount();
        // We don't reuse the builder as-is because it might have been built with a higher overhead ratio
        final PackedInts.Mutable reader = PackedInts.getMutable(builder.maxDoc(), PackedInts.bitsRequired(valueCount), acceptableOverheadRatio);
        PackedInts.copy(builder.getFirstOrdinals(), 0, reader, 0, builder.maxDoc(), 8 * 1024);
        this.reader = reader;
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.NUM_BYTES_OBJECT_REF + reader.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.singleton(Accountables.namedAccountable("reader", reader));
    }

    @Override
    public SortedSetDocValues ordinals(ValuesHolder values) {
        return (SortedSetDocValues) DocValues.singleton(new Docs(this, values));
    }

    private static class Docs extends AbstractSortedDocValues {

        private final int maxOrd;
        private final PackedInts.Reader reader;
        private final ValuesHolder values;

        private int currentDoc = -1;
        private int currentOrd;

        Docs(SinglePackedOrdinals parent, ValuesHolder values) {
            this.maxOrd = parent.valueCount;
            this.reader = parent.reader;
            this.values = values;
        }

        @Override
        public int getValueCount() {
            return maxOrd;
        }

        @Override
        public BytesRef lookupOrd(int ord) {
            return values.lookupOrd(ord);
        }

        @Override
        public int ordValue() {
            return currentOrd;
        }

        @Override
        public boolean advanceExact(int docID) throws IOException {
            currentDoc = docID;
            currentOrd = (int) (reader.get(docID) - 1);
            return currentOrd != -1;
        }

        @Override
        public int docID() {
            return currentDoc;
        }
    }
}
