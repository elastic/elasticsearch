/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class IPAddressField implements Field<IPAddress> {

    protected final String name;
    protected final FieldSupplier.Supplier<IPAddress> supplier;

    public IPAddressField(String name, FieldSupplier.Supplier<IPAddress> supplier) {
        this.name = name;
        this.supplier = supplier;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return supplier.size() == 0;
    }

    @Override
    public int size() {
        return supplier.size();
    }

    public IPAddress get(IPAddress defaultValue) {
        return get(0, defaultValue);
    }

    public IPAddress get(int index, IPAddress defaultValue) {
        if (isEmpty() || index < 0 || index >= size()) {
            return defaultValue;
        }

        return supplier.get(index);
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
                return supplier.get(index++);
            }
        };
    }

    public List<String> asStrings() {
        if (isEmpty()) {
            return Collections.emptyList();
        }

        List<String> values = new ArrayList<>(size());
        for (int i = 0; i < size(); i++) {
            values.add(supplier.get(i).toString());
        }

        return values;
    }

    public String asString(String defaultValue) {
        return asString(0, defaultValue);
    }

    public String asString(int index, String defaultValue) {
        if (isEmpty() || index < 0 || index >= supplier.size()) {
            return defaultValue;
        }

        return supplier.get(index).toString();
    }
}
