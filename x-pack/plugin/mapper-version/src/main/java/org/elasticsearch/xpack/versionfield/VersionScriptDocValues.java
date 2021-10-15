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

import java.io.IOException;

public final class VersionScriptDocValues extends ScriptDocValues<String> {

    private final SortedSetDocValues in;
    private long[] ords = new long[0];
    private int count;

    public VersionScriptDocValues(SortedSetDocValues in) {
        this.in = in;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        count = 0;
        if (in.advanceExact(docId)) {
            for (long ord = in.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = in.nextOrd()) {
                ords = ArrayUtil.grow(ords, count + 1);
                ords[count++] = ord;
            }
        }
    }

    public String getValue() {
        return get(0);
    }

    @Override
    public String get(int index) {
        if (count == 0) {
            throw new IllegalStateException(
                "A document doesn't have a value for a field! " + "Use doc[<field>].size()==0 to check if a document is missing a field!"
            );
        }
        try {
            return VersionEncoder.decodeVersion(in.lookupOrd(ords[index]));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int size() {
        return count;
    }
}
