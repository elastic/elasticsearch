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

import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

/**
 * Specialization of {@link LeafNumericFieldData} for integers.
 */
abstract class LeafLongFieldData implements LeafNumericFieldData {

    private final long ramBytesUsed;
    /**
     * Type of this field. Used to expose appropriate types in {@link #getScriptValues()}.
     */
    private final NumericType numericType;

    LeafLongFieldData(long ramBytesUsed, NumericType numericType) {
        this.ramBytesUsed = ramBytesUsed;
        this.numericType = numericType;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    @Override
    public final ScriptDocValues<?> getScriptValues() {
        switch (numericType) {
        // for now, dates and nanoseconds are treated the same, which also means, that the precision is only on millisecond level
        case DATE:
            return new ScriptDocValues.Dates(getLongValues(), false);
        case DATE_NANOSECONDS:
            assert this instanceof SortedNumericIndexFieldData.NanoSecondFieldData;
            return new ScriptDocValues.Dates(((SortedNumericIndexFieldData.NanoSecondFieldData) this).getLongValuesAsNanos(), true);
        case BOOLEAN:
            return new ScriptDocValues.Booleans(getLongValues());
        default:
            return new ScriptDocValues.Longs(getLongValues());
        }
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getLongValues());
    }

    @Override
    public final SortedNumericDoubleValues getDoubleValues() {
        return FieldData.castToDouble(getLongValues());
    }

    @Override
    public void close() {}
}
