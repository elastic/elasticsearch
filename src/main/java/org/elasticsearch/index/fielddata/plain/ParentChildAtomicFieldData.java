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

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.search.MultiValueMode;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 */
public class ParentChildAtomicFieldData extends AbstractAtomicParentChildFieldData {

    private final ImmutableOpenMap<String, AtomicOrdinalsFieldData> typeToIds;
    private final long memorySizeInBytes;

    public ParentChildAtomicFieldData(ImmutableOpenMap<String, AtomicOrdinalsFieldData> typeToIds) {
        this.typeToIds = typeToIds;
        long size = 0;
        for (ObjectCursor<AtomicOrdinalsFieldData> cursor : typeToIds.values()) {
            size += cursor.value.ramBytesUsed();
        }
        this.memorySizeInBytes = size;
    }

    @Override
    public long ramBytesUsed() {
        return memorySizeInBytes;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        // TODO: should we break down by type?
        // the current 'map' does not impl java.util.Map so we cant use Accountables.namedAccountables...
        return Collections.emptyList();
    }

    @Override
    public Set<String> types() {
        final Set<String> types = new HashSet<>();
        for (ObjectCursor<String> cursor : typeToIds.keys()) {
            types.add(cursor.value);
        }
        return types;
    }

    @Override
    public SortedDocValues getOrdinalsValues(String type) {
        AtomicOrdinalsFieldData atomicFieldData = typeToIds.get(type);
        if (atomicFieldData != null) {
            return MultiValueMode.MIN.select(atomicFieldData.getOrdinalsValues());
        } else {
            return DocValues.emptySorted();
        }
    }

    public AtomicOrdinalsFieldData getAtomicFieldData(String type) {
        return typeToIds.get(type);
    }

    @Override
    public void close() {
        for (ObjectCursor<AtomicOrdinalsFieldData> cursor : typeToIds.values()) {
            cursor.value.close();
        }
    }
}
