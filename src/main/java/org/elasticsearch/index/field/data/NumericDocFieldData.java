/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
 *
 */
public class NumericDocFieldData<T extends NumericFieldData> extends DocFieldData<T> {

    public NumericDocFieldData(T fieldData) {
        super(fieldData);
    }

    public int getIntValue() {
        return fieldData.intValue(docId);
    }

    public long getLongValue() {
        return fieldData.longValue(docId);
    }

    public float getFloatValue() {
        return fieldData.floatValue(docId);
    }

    public double getDoubleValue() {
        return fieldData.doubleValue(docId);
    }

    public short getShortValue() {
        return fieldData.shortValue(docId);
    }

    public byte getByteValue() {
        return fieldData.byteValue(docId);
    }

    public double[] getDoubleValues() {
        return fieldData.doubleValues(docId);
    }
}
