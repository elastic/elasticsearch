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
package org.elasticsearch.index.fielddata.fieldcomparator;


/**
 * Base FieldComparator class for number fields.
 */
// This is right now only used for sorting number based fields inside nested objects
public abstract class NumberComparatorBase<T> extends NestedWrappableComparator<T> {

    protected T top;
    /**
     * Adds numeric value at the specified doc to the specified slot.
     *
     * @param slot  The specified slot
     * @param doc   The specified doc
     */
    public abstract void add(int slot, int doc);

    /**
     * Divides the value at the specified slot with the specified divisor.
     *
     * @param slot      The specified slot
     * @param divisor   The specified divisor
     */
    public abstract void divide(int slot, int divisor);

    @Override
    public void setTopValue(T top) {
        this.top = top;
    }

}
