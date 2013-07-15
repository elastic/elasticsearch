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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;

/**
 *
 */
public class StringScriptDataComparator extends FieldComparator<BytesRef> {

    public static IndexFieldData.XFieldComparatorSource comparatorSource(SearchScript script) {
        return new InnerSource(script);
    }

    private static class InnerSource extends IndexFieldData.XFieldComparatorSource {

        private final SearchScript script;

        private InnerSource(SearchScript script) {
            this.script = script;
        }

        @Override
        public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
            return new StringScriptDataComparator(numHits, script);
        }

        @Override
        public SortField.Type reducedType() {
            return SortField.Type.STRING;
        }
    }

    private final SearchScript script;

    private BytesRef[] values; // TODO maybe we can preallocate or use a sentinel to prevent the conditionals in compare

    private BytesRef bottom;
    
    private final BytesRef spare = new BytesRef();
    
    private int spareDoc = -1;

    public StringScriptDataComparator(int numHits, SearchScript script) {
        this.script = script;
        values = new BytesRef[numHits];
    }

    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        script.setNextReader(context);
        spareDoc = -1;
        return this;
    }

    @Override
    public void setScorer(Scorer scorer) {
        script.setScorer(scorer);
    }

    @Override
    public int compare(int slot1, int slot2) {
        final BytesRef val1 = values[slot1];
        final BytesRef val2 = values[slot2];
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }

        return val1.compareTo(val2);
    }

    @Override
    public int compareBottom(int doc) {
        if (bottom == null) {
            return -1;
        }
        setSpare(doc);
        return bottom.compareTo(spare);
    }

    @Override
    public int compareDocToValue(int doc, BytesRef val2) throws IOException {
        script.setNextDocId(doc);
        setSpare(doc);
        return spare.compareTo(val2);
    }
    
    private void setSpare(int doc) {
        if (spareDoc == doc) {
            return;
        }
        script.setNextDocId(doc);
        spare.copyChars(script.run().toString());
        spareDoc = doc;
    }

    @Override
    public void copy(int slot, int doc) {
        setSpare(doc);
        if (values[slot] == null) {
           values[slot] = BytesRef.deepCopyOf(spare);
        } else {
            values[slot].copyBytes(spare);
        }
    }

    @Override
    public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override
    public BytesRef value(int slot) {
        return values[slot];
    }
}
