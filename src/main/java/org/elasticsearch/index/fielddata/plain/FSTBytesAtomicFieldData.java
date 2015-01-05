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

import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.Util;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 */
public class FSTBytesAtomicFieldData extends AbstractAtomicOrdinalsFieldData {

    // 0 ordinal in values means no value (its null)
    protected final Ordinals ordinals;

    private long size = -1;

    private final FST<Long> fst;

    public FSTBytesAtomicFieldData(FST<Long> fst, Ordinals ordinals) {
        this.ordinals = ordinals;
        this.fst = fst;
    }

    @Override
    public void close() {
    }

    @Override
    public long ramBytesUsed() {
        if (size == -1) {
            long size = ordinals.ramBytesUsed();
            // FST
            size += fst == null ? 0 : fst.ramBytesUsed();
            this.size = size;
        }
        return size;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        List<Accountable> resources = new ArrayList<>();
        resources.add(Accountables.namedAccountable("ordinals", ordinals));
        if (fst != null) {
            resources.add(Accountables.namedAccountable("terms", fst));
        }
        return Collections.unmodifiableList(resources);
    }

    @Override
    public RandomAccessOrds getOrdinalsValues() {
        return ordinals.ordinals(new ValuesHolder(fst));
    }

    private static class ValuesHolder implements Ordinals.ValuesHolder {

        private final FST<Long> fst;

        // per-thread resources
        private final BytesRefBuilder scratch;
        protected final BytesReader in;
        protected final Arc<Long> firstArc = new Arc<>();
        protected final Arc<Long> scratchArc = new Arc<>();
        protected final IntsRefBuilder scratchInts = new IntsRefBuilder();

        ValuesHolder(FST<Long> fst) {
            this.fst = fst;
            scratch = new BytesRefBuilder();
            in = fst.getBytesReader();
        }

        @Override
        public BytesRef lookupOrd(long ord) {
            assert ord != SortedSetDocValues.NO_MORE_ORDS;
            in.setPosition(0);
            fst.getFirstArc(firstArc);
            try {
                IntsRef output = Util.getByOutput(fst, ord, in, firstArc, scratchArc, scratchInts);
                scratch.clear();
                scratch.grow(output.length);
                Util.toBytesRef(output, scratch);
            } catch (IOException ex) {
                //bogus
            }
            return scratch.get();
        }
    }

}
