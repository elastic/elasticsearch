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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

import java.io.IOException;
import java.util.Comparator;

/**
 * A general {@link org.apache.lucene.index.TermsEnum} to iterate over terms from a {@link AtomicFieldData.WithOrdinals}
 * instance.
 */
public class AtomicFieldDataWithOrdinalsTermsEnum extends TermsEnum {

    private final BytesValues.WithOrdinals bytesValues;
    private final Ordinals.Docs ordinals;
    private final long maxOrd;

    private long currentOrd = Ordinals.MISSING_ORDINAL;
    private BytesRef currentTerm;

    public AtomicFieldDataWithOrdinalsTermsEnum(AtomicFieldData.WithOrdinals afd) {
        this.bytesValues = afd.getBytesValues(false);
        this.ordinals = bytesValues.ordinals();
        this.maxOrd = ordinals.getMaxOrd();
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
        long ord = binarySearch(bytesValues, text);
        if (ord >= 0) {
            currentOrd = ord;
            currentTerm = bytesValues.getValueByOrd(currentOrd);
            return SeekStatus.FOUND;
        } else {
            currentOrd = -ord - 1;
            if (ord >= maxOrd) {
                return SeekStatus.END;
            } else {
                currentTerm = bytesValues.getValueByOrd(currentOrd);
                return SeekStatus.NOT_FOUND;
            }
        }
    }

    @Override
    public void seekExact(long ord) throws IOException {
        assert ord >= 0 && ord < ordinals.getMaxOrd();
        currentOrd = ord;
        if (currentOrd == Ordinals.MISSING_ORDINAL) {
            currentTerm = null;
        } else {
            currentTerm = bytesValues.getValueByOrd(currentOrd);
        }
    }

    @Override
    public BytesRef term() throws IOException {
        return currentTerm;
    }

    @Override
    public long ord() throws IOException {
        return currentOrd;
    }

    @Override
    public int docFreq() throws IOException {
        throw new UnsupportedOperationException("docFreq not supported");
    }

    @Override
    public long totalTermFreq() throws IOException {
        return -1;
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
        throw new UnsupportedOperationException("docs not supported");
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
        throw new UnsupportedOperationException("docsAndPositions not supported");
    }

    @Override
    public BytesRef next() throws IOException {
        if (++currentOrd < maxOrd) {
            return currentTerm = bytesValues.getValueByOrd(currentOrd);
        } else {
            return null;
        }
    }

    @Override
    public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    final private static long binarySearch(BytesValues.WithOrdinals a, BytesRef key) {
        long low = 1;
        long high = a.ordinals().getMaxOrd();
        while (low <= high) {
            long mid = (low + high) >>> 1;
            BytesRef midVal = a.getValueByOrd(mid);
            int cmp;
            if (midVal != null) {
                cmp = midVal.compareTo(key);
            } else {
                cmp = -1;
            }

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

}
