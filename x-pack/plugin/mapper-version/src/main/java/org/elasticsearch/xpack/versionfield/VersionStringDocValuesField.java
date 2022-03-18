/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.field.AbstractScriptFieldFactory;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.Field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class VersionStringDocValuesField extends AbstractScriptFieldFactory<Version>
    implements
        Field<Version>,
        DocValuesScriptFieldFactory,
        ScriptDocValues.Supplier<String> {

    protected final SortedSetDocValues input;
    protected final String name;

    protected long[] ords = new long[0];
    protected int count;

    // used for backwards compatibility for old-style "doc" access
    // as a delegate to this field class
    private VersionScriptDocValues versionScriptDocValues = null;

    public VersionStringDocValuesField(SortedSetDocValues input, String name) {
        this.input = input;
        this.name = name;
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
    public String getInternal(int index) {
        try {
            return VersionEncoder.decodeVersion(input.lookupOrd(ords[index]));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public ScriptDocValues<?> toScriptDocValues() {
        if (versionScriptDocValues == null) {
            versionScriptDocValues = new VersionScriptDocValues(this);
        }

        return versionScriptDocValues;
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
        return count == 0;
    }

    public Version get(Version defaultValue) {
        return get(0, defaultValue);
    }

    public Version get(int index, Version defaultValue) {
        if (isEmpty() || index < 0 || index >= size()) {
            return defaultValue;
        }

        return new Version(getInternal(index));
    }

    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<Version> iterator() {
        return new Iterator<Version>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < size();
            }

            @Override
            public Version next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return new Version(getInternal(index++));
            }
        };
    }
}
