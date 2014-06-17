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

import org.apache.lucene.index.AtomicReader;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues.Strings;

/**
 * An {@link AtomicFieldData} implementation that uses Lucene {@link org.apache.lucene.index.SortedSetDocValues}.
 */
public final class SortedSetDVBytesAtomicFieldData extends SortedSetDVAtomicFieldData implements AtomicFieldData.WithOrdinals<ScriptDocValues.Strings> {

    /* NOTE: This class inherits the methods getBytesValues() and getHashedBytesValues()
     * from SortedSetDVAtomicFieldData. This can cause confusion since the are
     * part of the interface this class implements.*/

    SortedSetDVBytesAtomicFieldData(AtomicReader reader, String field) {
        super(reader, field);
    }

    @Override
    public Strings getScriptValues() {
        return new ScriptDocValues.Strings(getBytesValues());
    }

}
