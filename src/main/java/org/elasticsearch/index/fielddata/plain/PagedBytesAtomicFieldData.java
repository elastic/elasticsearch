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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 */
public class PagedBytesAtomicFieldData implements AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> {

    private final PagedBytes.Reader bytes;
    private final MonotonicAppendingLongBuffer termOrdToBytesOffset;
    protected final Ordinals ordinals;

    private long size = -1;
    private final long readerBytesSize;

    public PagedBytesAtomicFieldData(PagedBytes.Reader bytes, long readerBytesSize, MonotonicAppendingLongBuffer termOrdToBytesOffset, Ordinals ordinals) {
        this.bytes = bytes;
        this.termOrdToBytesOffset = termOrdToBytesOffset;
        this.ordinals = ordinals;
        this.readerBytesSize = readerBytesSize;
    }

    @Override
    public void close() {
    }

    @Override
    public long ramBytesUsed() {
        if (size == -1) {
            long size = ordinals.ramBytesUsed();
            // PackedBytes
            size += readerBytesSize;
            // PackedInts
            size += termOrdToBytesOffset.ramBytesUsed();
            this.size = size;
        }
        return size;
    }

    @Override
    public BytesValues.WithOrdinals getBytesValues() {
        return ordinals.ordinals(new ValuesHolder(bytes, termOrdToBytesOffset));
    }

    @Override
    public ScriptDocValues.Strings getScriptValues() {
        return new ScriptDocValues.Strings(getBytesValues());
    }

    private static class ValuesHolder implements Ordinals.ValuesHolder {

        private final BytesRef scratch = new BytesRef();
        private final PagedBytes.Reader bytes;
        private final MonotonicAppendingLongBuffer termOrdToBytesOffset;

        ValuesHolder(PagedBytes.Reader bytes, MonotonicAppendingLongBuffer termOrdToBytesOffset) {
            this.bytes = bytes;
            this.termOrdToBytesOffset = termOrdToBytesOffset;
        }

        @Override
        public BytesRef getValueByOrd(long ord) {
            assert ord != BytesValues.WithOrdinals.MISSING_ORDINAL;
            bytes.fill(scratch, termOrdToBytesOffset.get(ord));
            return scratch;
        }
        
    }

}
