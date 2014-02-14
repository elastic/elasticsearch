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
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 * An empty {@link org.elasticsearch.index.fielddata.BytesValues.WithOrdinals} implementation
 */
final class EmptyByteValuesWithOrdinals extends BytesValues.WithOrdinals {

    EmptyByteValuesWithOrdinals(Ordinals.Docs ordinals) {
        super(ordinals);
    }

    @Override
    public BytesRef getValueByOrd(long ord) {
        scratch.length = 0;
        return scratch;
    }

    @Override
    public int setDocument(int docId) {
        return 0;
    }

    @Override
    public BytesRef nextValue() {
        throw new ElasticsearchIllegalStateException("Empty BytesValues has no next value");
    }

    @Override
    public int currentValueHash() {
        throw new ElasticsearchIllegalStateException("Empty BytesValues has no hash for the current value");
    }

}