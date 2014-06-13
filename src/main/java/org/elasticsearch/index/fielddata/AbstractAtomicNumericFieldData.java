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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.BytesRef;

/**
 */
public abstract class AbstractAtomicNumericFieldData implements AtomicNumericFieldData {

    private boolean isFloat;

    public AbstractAtomicNumericFieldData(boolean isFloat) {
        this.isFloat = isFloat;
    }

    @Override
    public ScriptDocValues getScriptValues() {
        if (isFloat) {
            return new ScriptDocValues.Doubles(getDoubleValues());
        } else {
            return new ScriptDocValues.Longs(getLongValues());
        }
    }

    @Override
    public BytesValues getBytesValues() {
        if (isFloat) {
            final DoubleValues values = getDoubleValues();
            return new BytesValues(values.isMultiValued()) {
                private final BytesRef scratch = new BytesRef();

                @Override
                public int setDocument(int docId) {
                    return values.setDocument(docId);
                }

                @Override
                public BytesRef nextValue() {
                    scratch.copyChars(Double.toString(values.nextValue()));
                    return scratch;
                }

                @Override
                public Order getOrder() {
                    return values.getOrder();
                }

            };
        } else {
            final LongValues values = getLongValues();
            return new BytesValues(values.isMultiValued()) {
                private final BytesRef scratch = new BytesRef();

                @Override
                public int setDocument(int docId) {
                    return values.setDocument(docId);
                }

                @Override
                public BytesRef nextValue() {
                    scratch.copyChars(Long.toString(values.nextValue()));
                    return scratch;
                }

                @Override
                public Order getOrder() {
                    return values.getOrder();
                }
            };
        }
    }
}
