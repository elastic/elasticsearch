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

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

import java.util.Collection;
import java.util.List;

public class PagedBytesAtomicFieldData extends AbstractAtomicOrdinalsFieldData {

    private final PagedBytes.Reader bytes;
    private final PackedLongValues termOrdToBytesOffset;
    protected final Ordinals ordinals;

    public PagedBytesAtomicFieldData(PagedBytes.Reader bytes, PackedLongValues termOrdToBytesOffset, Ordinals ordinals) {
        super(DEFAULT_SCRIPT_FUNCTION);
        this.bytes = bytes;
        this.termOrdToBytesOffset = termOrdToBytesOffset;
        this.ordinals = ordinals;
    }

    @Override
    public void close() {
    }

    @Override
    public long ramBytesUsed() {
        long size = ordinals.ramBytesUsed();
        // PackedBytes
        size += bytes.ramBytesUsed();
        // PackedInts
        size += termOrdToBytesOffset.ramBytesUsed();
        return size;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return List.of(
                Accountables.namedAccountable("ordinals", ordinals),
                Accountables.namedAccountable("term bytes", bytes),
                Accountables.namedAccountable("term offsets", termOrdToBytesOffset));
    }

    @Override
    public SortedSetDocValues getOrdinalsValues() {
        return ordinals.ordinals(new ValuesHolder(bytes, termOrdToBytesOffset));
    }

    private static class ValuesHolder implements Ordinals.ValuesHolder {

        private final BytesRef scratch = new BytesRef();
        private final PagedBytes.Reader bytes;
        private final PackedLongValues termOrdToBytesOffset;

        ValuesHolder(PagedBytes.Reader bytes, PackedLongValues termOrdToBytesOffset) {
            this.bytes = bytes;
            this.termOrdToBytesOffset = termOrdToBytesOffset;
        }

        @Override
        public BytesRef lookupOrd(long ord) {
            assert ord >= 0;
            bytes.fill(scratch, termOrdToBytesOffset.get(ord));
            return scratch;
        }

    }

}
