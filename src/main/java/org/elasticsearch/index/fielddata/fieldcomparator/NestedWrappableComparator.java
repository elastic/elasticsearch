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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.search.FieldComparator;

/** Base comparator which allows for nested sorting. */
public abstract class NestedWrappableComparator<T> extends FieldComparator<T> {

    /**
     * Assigns the underlying missing value to the specified slot, if the actual implementation supports missing value.
     *
     * @param slot The slot to assign the the missing value to.
     */
    public abstract void missing(int slot);

    /**
     * Compares the missing value to the bottom.
     *
     * @return any N < 0 if the bottom value is not competitive with the missing value, any N > 0 if the
     * bottom value is competitive with the missing value and 0 if they are equal.
     */
    public abstract int compareBottomMissing();

}
