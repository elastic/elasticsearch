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

//////////////////////////////////////////////////
// THIS IS A GENERATED CLASS. DO NOT HAND EDIT! //
//////////////////////////////////////////////////


/**
 * Iterator for maps of type Object and double.
 * <p/>
 * <p>The iterator semantics for Trove's primitive maps is slightly different
 * from those defined in <tt>java.util.Iterator</tt>, but still well within
 * the scope of the pattern, as defined by Gamma, et al.</p>
 * <p/>
 * <p>This iterator does <b>not</b> implicitly advance to the next entry when
 * the value at the current position is retrieved.  Rather, you must explicitly
 * ask the iterator to <tt>advance()</tt> and then retrieve either the <tt>key()</tt>,
 * the <tt>value()</tt> or both.  This is done so that you have the option, but not
 * the obligation, to retrieve keys and/or values as your application requires, and
 * without introducing wrapper objects that would carry both.  As the iteration is
 * stateful, access to the key/value parts of the current map entry happens in
 * constant time.</p>
 * <p/>
 * <p>In practice, the iterator is akin to a "search finger" that you move from
 * position to position.  Read or write operations affect the current entry only and
 * do not assume responsibility for moving the finger.</p>
 * <p/>
 * <p>Here are some sample scenarios for this class of iterator:</p>
 * <p/>
 * <pre>
 * // accessing keys/values through an iterator:
 * for (TObjectDoubleIterator it = map.iterator();
 *      it.hasNext();) {
 *   it.advance();
 *   if (satisfiesCondition(it.key()) {
 *     doSomethingWithValue(it.value());
 *   }
 * }
 * </pre>
 * <p/>
 * <pre>
 * // modifying values in-place through iteration:
 * for (TObjectDoubleIterator it = map.iterator();
 *      it.hasNext();) {
 *   it.advance();
 *   if (satisfiesCondition(it.key()) {
 *     it.setValue(newValueForKey(it.key()));
 *   }
 * }
 * </pre>
 * <p/>
 * <pre>
 * // deleting entries during iteration:
 * for (TObjectDoubleIterator it = map.iterator();
 *      it.hasNext();) {
 *   it.advance();
 *   if (satisfiesCondition(it.key()) {
 *     it.remove();
 *   }
 * }
 * </pre>
 * <p/>
 * <pre>
 * // faster iteration by avoiding hasNext():
 * TObjectDoubleIterator iterator = map.iterator();
 * for (int i = map.size(); i-- > 0;) {
 *   iterator.advance();
 *   doSomethingWithKeyAndValue(iterator.key(), iterator.value());
 * }
 * </pre>
 *
 * @author Eric D. Friedman
 * @version $Id: O2PIterator.template,v 1.3 2007/01/22 16:56:39 robeden Exp $
 */

public class TObjectDoubleIterator<K> extends TIterator {
    private final TObjectDoubleHashMap<K> _map;

    public TObjectDoubleIterator(TObjectDoubleHashMap<K> map) {
        super(map);
        this._map = map;
    }

    /**
     * Returns the index of the next value in the data structure
     * or a negative value if the iterator is exhausted.
     *
     * @return an <code>double</code> value
     */
    protected final int nextIndex() {
        if (_expectedSize != _hash.size()) {
            throw new ConcurrentModificationException();
        }

        Object[] set = _map._set;
        int i = _index;
        while (i-- > 0 && (set[i] == null || set[i] == TObjectHash.REMOVED ||
                set[i] == TObjectHash.FREE)) ;
        return i;
    }

    /**
     * Moves the iterator forward to the next entry in the underlying map.
     *
     * @throws java.util.NoSuchElementException
     *          if the iterator is already exhausted
     */
    public void advance() {
        moveToNextIndex();
    }

    /**
     * Provides access to the key of the mapping at the iterator's position.
     * Note that you must <tt>advance()</tt> the iterator at least once
     * before invoking this method.
     *
     * @return the key of the entry at the iterator's current position.
     */
    public K key() {
        return (K) _map._set[_index];
    }

    /**
     * Provides access to the value of the mapping at the iterator's position.
     * Note that you must <tt>advance()</tt> the iterator at least once
     * before invoking this method.
     *
     * @return the value of the entry at the iterator's current position.
     */
    public double value() {
        return _map._values[_index];
    }

    /**
     * Replace the value of the mapping at the iterator's position with the
     * specified value. Note that you must <tt>advance()</tt> the iterator at
     * least once before invoking this method.
     *
     * @param val the value to set in the current entry
     * @return the old value of the entry.
     */
    public double setValue(double val) {
        double old = value();
        _map._values[_index] = val;
        return old;
    }
}// TObjectDoubleIterator
