/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.script.field.ToScriptFieldFactory;

import java.util.Collection;
import java.util.List;

public class PagedBytesLeafFieldData extends AbstractLeafOrdinalsFieldData {

    private final PagedBytes.Reader bytes;
    private final PackedLongValues termOrdToBytesOffset;
    protected final Ordinals ordinals;

    public PagedBytesLeafFieldData(
        PagedBytes.Reader bytes,
        PackedLongValues termOrdToBytesOffset,
        Ordinals ordinals,
        ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory
    ) {
        super(toScriptFieldFactory);
        this.bytes = bytes;
        this.termOrdToBytesOffset = termOrdToBytesOffset;
        this.ordinals = ordinals;
    }

    @Override
    public void close() {}

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
            Accountables.namedAccountable("term offsets", termOrdToBytesOffset)
        );
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
