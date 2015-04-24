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

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;

/**
 * A thread safe ordinals abstraction. Ordinals can only be positive integers.
 */
public abstract class Ordinals implements Accountable {

    public static final ValuesHolder NO_VALUES = new ValuesHolder() {
        @Override
        public BytesRef lookupOrd(long ord) {
            throw new UnsupportedOperationException();
        }
    };

    /**
     * The memory size this ordinals take.
     */
    @Override
    public abstract long ramBytesUsed();

    public abstract RandomAccessOrds ordinals(ValuesHolder values);

    public final RandomAccessOrds ordinals() {
        return ordinals(NO_VALUES);
    }

    public static interface ValuesHolder {

        public abstract BytesRef lookupOrd(long ord);

    }

}
