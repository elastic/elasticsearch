/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;

/**
 * VectorScriptDocValues represents docValues for dense and sparse vector fields
 */
public final class VectorScriptDocValues extends ScriptDocValues<BytesRef> {

    private final BinaryDocValues in;
    private BytesRef value;

    VectorScriptDocValues(BinaryDocValues in) {
        this.in = in;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (in.advanceExact(docId)) {
            value = in.binaryValue();
        } else {
            value = null;
        }
    }

    public BytesRef getValue() {
        return value;
    }

    @Override
    public BytesRef get(int index) {
        throw new UnsupportedOperationException("this operation is not supported on vector doc values");
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("this operation is not supported on vector doc values");
    }

}
