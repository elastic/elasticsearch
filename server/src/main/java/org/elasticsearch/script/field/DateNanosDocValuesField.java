/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DateNanosDocValuesField extends AbstractScriptFieldFactory<ZonedDateTime>
    implements
        Field<ZonedDateTime>,
        DocValuesScriptFieldFactory,
        ScriptDocValues.Supplier<ZonedDateTime> {

    protected final SortedNumericDocValues input;
    protected final String name;

    protected ZonedDateTime[] values = new ZonedDateTime[0];
    protected int count;

    private ScriptDocValues.Dates dates = null;

    public DateNanosDocValuesField(SortedNumericDocValues input, String name) {
        this.input = input;
        this.name = name;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            for (int i = 0; i < count; i++) {
                values[i] = ZonedDateTime.ofInstant(DateUtils.toInstant(input.nextValue()), ZoneOffset.UTC);
            }
        } else {
            resize(0);
        }
    }

    protected void resize(int newSize) {
        count = newSize;

        assert count >= 0 : "size must be positive (got " + count + "): likely integer overflow?";
        values = ArrayUtil.grow(values, count);
    }

    @Override
    public ScriptDocValues<ZonedDateTime> toScriptDocValues() {
        if (dates == null) {
            dates = new ScriptDocValues.Dates(this);
        }

        return dates;
    }

    @Override
    public ZonedDateTime getInternal(int index) {
        return values[index];
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return count == 0;
    }

    @Override
    public int size() {
        return count;
    }

    public ZonedDateTime get(ZonedDateTime defaultValue) {
        return get(0, defaultValue);
    }

    public ZonedDateTime get(int index, ZonedDateTime defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return values[index];
    }

    @Override
    public Iterator<ZonedDateTime> iterator() {
        return new Iterator<ZonedDateTime>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public ZonedDateTime next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return values[index++];
            }
        };
    }
}
