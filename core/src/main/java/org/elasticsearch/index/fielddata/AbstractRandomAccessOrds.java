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

import org.apache.lucene.index.RandomAccessOrds;

/**
 * Base implementation of a {@link RandomAccessOrds} instance.
 */
public abstract class AbstractRandomAccessOrds extends RandomAccessOrds {

    int i = 0;

    protected abstract void doSetDocument(int docID);

    @Override
    public final void setDocument(int docID) {
        doSetDocument(docID);
        i = 0;
    }

    @Override
    public long nextOrd() {
        if (i < cardinality()) {
            return ordAt(i++);
        } else {
            return NO_MORE_ORDS;
        }
    }

}
