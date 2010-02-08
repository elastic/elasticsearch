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

package org.elasticsearch.util.gnu.trove;

import java.util.ConcurrentModificationException;

/**
 * Implements all iterator functions for the hashed object set.
 * Subclasses may override objectAtIndex to vary the object
 * returned by calls to next() (e.g. for values, and Map.Entry
 * objects).
 * <p/>
 * <p> Note that iteration is fastest if you forego the calls to
 * <tt>hasNext</tt> in favor of checking the size of the structure
 * yourself and then call next() that many times:
 * <p/>
 * <pre>
 * Iterator i = collection.iterator();
 * for (int size = collection.size(); size-- > 0;) {
 *   Object o = i.next();
 * }
 * </pre>
 * <p/>
 * <p>You may, of course, use the hasNext(), next() idiom too if
 * you aren't in a performance critical spot.</p>
 */
abstract class TPrimitiveIterator extends TIterator {
    /**
     * the collection on which this iterator operates.
     */
    protected final TPrimitiveHash _hash;

    /**
     * Creates a TPrimitiveIterator for the specified collection.
     */
    public TPrimitiveIterator(TPrimitiveHash hash) {
        super(hash);
        _hash = hash;
    }

    /**
     * Returns the index of the next value in the data structure
     * or a negative value if the iterator is exhausted.
     *
     * @return an <code>int</code> value
     * @throws ConcurrentModificationException
     *          if the underlying collection's
     *          size has been modified since the iterator was created.
     */
    protected final int nextIndex() {
        if (_expectedSize != _hash.size()) {
            throw new ConcurrentModificationException();
        }

        byte[] states = _hash._states;
        int i = _index;
        while (i-- > 0 && (states[i] != TPrimitiveHash.FULL)) ;
        return i;
    }

} // TPrimitiveIterator
