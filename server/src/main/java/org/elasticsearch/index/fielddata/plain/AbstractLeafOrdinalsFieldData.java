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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;


public abstract class AbstractLeafOrdinalsFieldData implements LeafOrdinalsFieldData {

    public static final Function<SortedSetDocValues, ScriptDocValues<?>> DEFAULT_SCRIPT_FUNCTION =
            ((Function<SortedSetDocValues, SortedBinaryDocValues>) FieldData::toString)
            .andThen(ScriptDocValues.Strings::new);

    private final Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction;

    protected AbstractLeafOrdinalsFieldData(Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction) {
        this.scriptFunction = scriptFunction;
    }

    @Override
    public final ScriptDocValues<?> getScriptValues() {
        return scriptFunction.apply(getOrdinalsValues());
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getOrdinalsValues());
    }

    public static LeafOrdinalsFieldData empty() {
        return new AbstractLeafOrdinalsFieldData(DEFAULT_SCRIPT_FUNCTION) {

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

            @Override
            public void close() {
            }

            @Override
            public SortedSetDocValues getOrdinalsValues() {
                return DocValues.emptySortedSet();
            }
        };
    }
}
