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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.FieldData;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * An {@link AtomicFieldData} implementation that uses Lucene {@link org.apache.lucene.index.SortedSetDocValues}.
 */
public final class SortedSetDVBytesAtomicFieldData extends AbstractAtomicOrdinalsFieldData {

    private final LeafReader reader;
    private final String field;

    SortedSetDVBytesAtomicFieldData(LeafReader reader, String field) {
        this.reader = reader;
        this.field = field;
    }

    @Override
    public RandomAccessOrds getOrdinalsValues() {
        try {
            return FieldData.maybeSlowRandomAccessOrds(DocValues.getSortedSet(reader, field));
        } catch (IOException e) {
            throw new IllegalStateException("cannot load docvalues", e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public long ramBytesUsed() {
        return 0; // unknown
    }
    
    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

}
