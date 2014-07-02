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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;

/**
 * Clone of {@link SortedNumericDocValues} for double values.
 */
public abstract class SortedNumericDoubleValues {

    /** Sole constructor. (For invocation by subclass
     * constructors, typically implicit.) */
    protected SortedNumericDoubleValues() {}

    /**
     * Positions to the specified document
     */
    public abstract void setDocument(int doc);

    /**
     * Retrieve the value for the current document at the specified index.
     * An index ranges from {@code 0} to {@code count()-1}.
     */
    public abstract double valueAt(int index);

    /**
     * Retrieves the count of values for the current document.
     * This may be zero if a document has no values.
     */
    public abstract int count();

}
