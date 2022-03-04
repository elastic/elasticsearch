/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;

public class IpAddressSortedSetDocValuesSupplier implements DocValuesSupplier<String>, FieldSupplier.Supplier<IPAddress> {

    private final SortedSetDocValues input;

    private long[] ords = new long[0];
    private int count;

    public IpAddressSortedSetDocValuesSupplier(SortedSetDocValues input) {
        this.input = input;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        count = 0;
        if (input.advanceExact(docId)) {
            for (long ord = input.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = input.nextOrd()) {
                ords = ArrayUtil.grow(ords, count + 1);
                ords[count++] = ord;
            }
        }
    }

    @Override
    public String getCompatible(int index) {
        try {
            BytesRef encoded = input.lookupOrd(ords[index]);
            InetAddress ia = InetAddressPoint.decode(Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length));
            return InetAddresses.toAddrString(ia);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public IPAddress get(int index) {
        try {
            BytesRef encoded = input.lookupOrd(ords[index]);
            InetAddress ia = InetAddressPoint.decode(Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length));
            return new IPAddress(ia);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}
