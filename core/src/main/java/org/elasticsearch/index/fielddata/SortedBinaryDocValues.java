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

import org.apache.lucene.util.BytesRef;

/**
 * A list of per-document binary values, sorted
 * according to {@link BytesRef#getUTF8SortedAsUnicodeComparator()}.
 * There might be dups however.
 */
public abstract class SortedBinaryDocValues {

    /**
     * Positions to the specified document
     */
    public abstract void setDocument(int docId);

    /**
     * Return the number of values of the current document.
     */
    public abstract int count();

    /**
     * Retrieve the value for the current document at the specified index.
     * An index ranges from {@code 0} to {@code count()-1}.
     * Note that the returned {@link BytesRef} might be reused across invocations.
     */
    public abstract BytesRef valueAt(int index);

}
