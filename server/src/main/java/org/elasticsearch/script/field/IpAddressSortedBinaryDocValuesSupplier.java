/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;

public class IpAddressSortedBinaryDocValuesSupplier implements DocValuesSupplier<String>, FieldSupplier.Supplier<IPAddress> {

    protected final SortedBinaryDocValues input;

    protected BytesRefBuilder[] values = new BytesRefBuilder[0];
    protected int count;

    public IpAddressSortedBinaryDocValuesSupplier(SortedBinaryDocValues input) {
        this.input = input;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            for (int i = 0; i < count; i++) {
                // We need to make a copy here, because BytesBinaryDVLeafFieldData's SortedBinaryDocValues
                // implementation reuses the returned BytesRef. Otherwise we would end up with the same BytesRef
                // instance for all slots in the values array.
                values[i].copyBytes(input.nextValue());
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
    public String getCompatible(int index) {
        return InetAddresses.toAddrString(InetAddressPoint.decode(values[index].bytes()));
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public IPAddress get(int index) {
        return new IPAddress(InetAddressPoint.decode(values[index].bytes()));
    }
}
