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
import java.util.Iterator;
import java.util.NoSuchElementException;

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
abstract class THashIterator<V> extends TIterator implements Iterator<V> {
    private final TObjectHash _object_hash;

    /**
     * Create an instance of THashIterator over the values of the TObjectHash
     */
    public THashIterator(TObjectHash hash) {
        super(hash);
        _object_hash = hash;
    }

    /**
     * Moves the iterator to the next Object and returns it.
     *
     * @return an <code>Object</code> value
     * @throws ConcurrentModificationException
     *                                if the structure
     *                                was changed using a method that isn't on this iterator.
     * @throws NoSuchElementException if this is called on an
     *                                exhausted iterator.
     */
    public V next() {
        moveToNextIndex();
        return objectAtIndex(_index);
    }

    /**
     * Returns the index of the next value in the data structure
     * or a negative value if the iterator is exhausted.
     *
     * @return an <code>int</code> value
     * @throws ConcurrentModificationException
     *          if the underlying
     *          collection's size has been modified since the iterator was
     *          created.
     */
    protected final int nextIndex() {
        if (_expectedSize != _hash.size()) {
            throw new ConcurrentModificationException();
        }

        Object[] set = _object_hash._set;
        int i = _index;
        while (i-- > 0 && (set[i] == TObjectHash.FREE || set[i] == TObjectHash.REMOVED)) ;
        return i;
    }

    /**
     * Returns the object at the specified index.  Subclasses should
     * implement this to return the appropriate object for the given
     * index.
     *
     * @param index the index of the value to return.
     * @return an <code>Object</code> value
     */
    abstract protected V objectAtIndex(int index);
} // THashIterator
