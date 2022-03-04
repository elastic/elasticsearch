/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DateMillisField implements Field<ZonedDateTime> {

    protected final String name;
    protected final FieldSupplier.Supplier<ZonedDateTime> supplier;

    public DateMillisField(String name, FieldSupplier.Supplier<ZonedDateTime> supplier) {
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

    public ZonedDateTime get(ZonedDateTime defaultValue) {
        return get(0, defaultValue);
    }

    public ZonedDateTime get(int index, ZonedDateTime defaultValue) {
        if (isEmpty() || index < 0 || index >= supplier.size()) {
            return defaultValue;
        }

        return supplier.get(index);
    }

    @Override
    public Iterator<ZonedDateTime> iterator() {
        return new Iterator<ZonedDateTime>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < supplier.size();
            }

            @Override
            public ZonedDateTime next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return supplier.get(index++);
            }
        };
    }
}
