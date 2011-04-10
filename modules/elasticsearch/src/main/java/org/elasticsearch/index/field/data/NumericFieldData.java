/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.field.data;

/**
 * @author kimchy (shay.banon)
 */
public abstract class NumericFieldData<Doc extends NumericDocFieldData> extends FieldData<Doc> {

    protected NumericFieldData(String fieldName) {
        super(fieldName);
    }

    /**
     * Returns the value of the specified number as an <code>int</code>.
     * This may involve rounding or truncation.
     *
     * @return the numeric value represented by this object after conversion
     *         to type <code>int</code>.
     */
    public abstract int intValue(int docId);

    /**
     * Returns the value of the specified number as a <code>long</code>.
     * This may involve rounding or truncation.
     *
     * @return the numeric value represented by this object after conversion
     *         to type <code>long</code>.
     */
    public abstract long longValue(int docId);

    /**
     * Returns the value of the specified number as a <code>float</code>.
     * This may involve rounding.
     *
     * @return the numeric value represented by this object after conversion
     *         to type <code>float</code>.
     */
    public abstract float floatValue(int docId);

    /**
     * Returns the value of the specified number as a <code>double</code>.
     * This may involve rounding.
     *
     * @return the numeric value represented by this object after conversion
     *         to type <code>double</code>.
     */
    public abstract double doubleValue(int docId);

    /**
     * Returns the value of the specified number as a <code>byte</code>.
     * This may involve rounding or truncation.
     *
     * @return the numeric value represented by this object after conversion
     *         to type <code>byte</code>.
     */
    public byte byteValue(int docId) {
        return (byte) intValue(docId);
    }

    /**
     * Returns the value of the specified number as a <code>short</code>.
     * This may involve rounding or truncation.
     *
     * @return the numeric value represented by this object after conversion
     *         to type <code>short</code>.
     */
    public short shortValue(int docId) {
        return (short) intValue(docId);
    }

    @Override public Doc docFieldData(int docId) {
        return super.docFieldData(docId);
    }

    public abstract double[] doubleValues(int docId);

    public abstract void forEachValueInDoc(int docId, DoubleValueInDocProc proc);

    public abstract void forEachValueInDoc(int docId, MissingDoubleValueInDocProc proc);

    public abstract void forEachValueInDoc(int docId, LongValueInDocProc proc);

    public abstract void forEachValueInDoc(int docId, MissingLongValueInDocProc proc);

    public static interface DoubleValueInDocProc {
        void onValue(int docId, double value);
    }

    public static interface LongValueInDocProc {
        void onValue(int docId, long value);
    }

    public static interface MissingLongValueInDocProc {
        void onValue(int docId, long value);

        void onMissing(int docId);
    }

    public static interface MissingDoubleValueInDocProc {
        void onValue(int docId, double value);

        void onMissing(int docId);
    }
}
