/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.fielddata.IpScriptFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class IpDocValuesField extends AbstractScriptFieldFactory<IPAddress>
    implements
        Field<IPAddress>,
        DocValuesScriptFieldFactory,
        ScriptDocValues.Supplier<String> {
    protected final String name;
    protected final ScriptDocValues.Supplier<InetAddress> raw;

    // used for backwards compatibility for old-style "doc" access
    // as a delegate to this field class
    protected ScriptDocValues.Strings strings = null;

    public IpDocValuesField(SortedSetDocValues input, String name) {
        this.name = name;
        this.raw = new SortedSetIpSupplier(input);
    }

    public IpDocValuesField(SortedBinaryDocValues input, String name) {
        this.name = name;
        this.raw = new SortedBinaryIpSupplier(input);
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        raw.setNextDocId(docId);
    }

    @Override
    public String getInternal(int index) {
        return InetAddresses.toAddrString(raw.getInternal(index));
    }

    @Override
    public ScriptDocValues<String> toScriptDocValues() {
        if (strings == null) {
            strings = new ScriptDocValues.Strings(this);
        }

        return strings;
    }

    public String asString(String defaultValue) {
        return asString(0, defaultValue);
    }

    public String asString(int index, String defaultValue) {
        if (isEmpty() || index < 0 || index >= size()) {
            return defaultValue;
        }

        return getInternal(index);
    }

    public List<String> asStrings() {
        if (isEmpty()) {
            return Collections.emptyList();
        }

        List<String> values = new ArrayList<>(size());
        for (int i = 0; i < size(); i++) {
            values.add(getInternal(i));
        }

        return values;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int size() {
        return raw.size();
    }

    public IPAddress get(IPAddress defaultValue) {
        return get(0, defaultValue);
    }

    public IPAddress get(int index, IPAddress defaultValue) {
        if (isEmpty() || index < 0 || index >= size()) {
            return defaultValue;
        }

        return new IPAddress(raw.getInternal(index));
    }

    @Override
    public Iterator<IPAddress> iterator() {
        return new Iterator<IPAddress>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < size();
            }

            @Override
            public IPAddress next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return new IPAddress(raw.getInternal(index++));
            }
        };
    }

    /** Used if we have access to global ordinals */
    protected static class SortedSetIpSupplier implements ScriptDocValues.Supplier<InetAddress> {
        private final SortedSetDocValues in;
        private long[] ords = new long[0];
        private int count;

        public SortedSetIpSupplier(SortedSetDocValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            count = 0;
            if (in.advanceExact(docId)) {
                for (int i = 0; i < in.docValueCount(); i++) {
                    long ord = in.nextOrd();
                    ords = ArrayUtil.grow(ords, count + 1);
                    ords[count++] = ord;
                }
            }
        }

        @Override
        public InetAddress getInternal(int index) {
            try {
                BytesRef encoded = in.lookupOrd(ords[index]);
                return InetAddressPoint.decode(Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int size() {
            return count;
        }
    }

    /** Used if we do not have global ordinals, such as in the IP runtime field see: {@link IpScriptFieldData} */
    protected static class SortedBinaryIpSupplier implements ScriptDocValues.Supplier<InetAddress> {
        private final SortedBinaryDocValues in;
        private BytesRefBuilder[] values = new BytesRefBuilder[0];
        private int count;

        public SortedBinaryIpSupplier(SortedBinaryDocValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    // We need to make a copy here, because BytesBinaryDVLeafFieldData's SortedBinaryDocValues
                    // implementation reuses the returned BytesRef. Otherwise we would end up with the same BytesRef
                    // instance for all slots in the values array.
                    values[i].copyBytes(in.nextValue());
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        private void resize(int newSize) {
            count = newSize;
            if (newSize > values.length) {
                final int oldLength = values.length;
                values = ArrayUtil.grow(values, count);
                for (int i = oldLength; i < values.length; ++i) {
                    values[i] = new BytesRefBuilder();
                }
            }
        }

        @Override
        public InetAddress getInternal(int index) {
            return InetAddressPoint.decode(BytesReference.toBytes(new BytesArray(values[index].toBytesRef())));
        }

        @Override
        public int size() {
            return count;
        }
    }
}
