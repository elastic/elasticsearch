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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Wraps a {@link LegacyBinaryDocValues} into a {@link BinaryDocValues}.
 *
 * @deprecated Implement {@link BinaryDocValues} directly.
 */
@Deprecated
public final class LegacyBinaryDocValuesWrapper extends BinaryDocValues {
    private final Bits docsWithField;
    private final LegacyBinaryDocValues values;
    private final int maxDoc;
    private int docID = -1;

    public LegacyBinaryDocValuesWrapper(Bits docsWithField, LegacyBinaryDocValues values) {
        this.docsWithField = docsWithField;
        this.values = values;
        this.maxDoc = docsWithField.length();
    }

    @Override
    public int docID() {
        return docID;
    }

    @Override
    public int nextDoc() {
        docID++;
        while (docID < maxDoc) {
            if (docsWithField.get(docID)) {
                return docID;
            }
            docID++;
        }
        docID = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) {
        if (target < docID) {
            throw new IllegalArgumentException("cannot advance backwards: docID=" + docID + " target=" + target);
        }
        if (target == NO_MORE_DOCS) {
            this.docID = NO_MORE_DOCS;
        } else {
            this.docID = target - 1;
            nextDoc();
        }
        return docID;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        docID = target;
        return docsWithField.get(target);
    }

    @Override
    public long cost() {
        return 0;
    }

    @Override
    public BytesRef binaryValue() {
        return values.get(docID);
    }
}
