/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.index;

import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;

/**
 * Wraps a {@link LegacySortedNumericDocValues} into a {@link SortedNumericDocValues}.
 *
 * @deprecated Implement {@link SortedNumericDocValues} directly.
 */
@Deprecated
public final class LegacySortedNumericDocValuesWrapper extends SortedNumericDocValues {
    private final LegacySortedNumericDocValues values;
    private final int maxDoc;
    private int docID = -1;
    private int upto;

    public LegacySortedNumericDocValuesWrapper(LegacySortedNumericDocValues values, int maxDoc) {
        this.values = values;
        this.maxDoc = maxDoc;
    }

    @Override
    public int docID() {
        return docID;
    }

    @Override
    public int nextDoc() {
        assert docID != NO_MORE_DOCS;
        while (true) {
            docID++;
            if (docID == maxDoc) {
                docID = NO_MORE_DOCS;
                break;
            }
            values.setDocument(docID);
            if (values.count() != 0) {
                break;
            }
        }
        upto = 0;
        return docID;
    }

    @Override
    public int advance(int target) {
        if (target < docID) {
            throw new IllegalArgumentException("cannot advance backwards: docID=" + docID + " target=" + target);
        }
        if (target >= maxDoc) {
            docID = NO_MORE_DOCS;
        } else {
            docID = target - 1;
            nextDoc();
        }
        return docID;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        docID = target;
        values.setDocument(docID);
        upto = 0;
        return values.count() != 0;
    }

    @Override
    public long cost() {
        return 0;
    }

    @Override
    public long nextValue() {
        return values.valueAt(upto++);
    }

    @Override
    public int docValueCount() {
        return values.count();
    }
}
