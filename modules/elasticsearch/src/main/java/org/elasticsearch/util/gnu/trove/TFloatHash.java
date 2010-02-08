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

//////////////////////////////////////////////////
// THIS IS A GENERATED CLASS. DO NOT HAND EDIT! //
//////////////////////////////////////////////////


/**
 * An open addressed hashing implementation for float primitives.
 * <p/>
 * Created: Sun Nov  4 08:56:06 2001
 *
 * @author Eric D. Friedman
 * @version $Id: PHash.template,v 1.2 2007/06/29 22:39:46 robeden Exp $
 */

abstract public class TFloatHash extends TPrimitiveHash implements TFloatHashingStrategy {

    /**
     * the set of floats
     */
    protected transient float[] _set;

    /**
     * strategy used to hash values in this collection
     */
    protected TFloatHashingStrategy _hashingStrategy;

    /**
     * Creates a new <code>TFloatHash</code> instance with the default
     * capacity and load factor.
     */
    public TFloatHash() {
        super();
        this._hashingStrategy = this;
    }

    /**
     * Creates a new <code>TFloatHash</code> instance whose capacity
     * is the next highest prime above <tt>initialCapacity + 1</tt>
     * unless that value is already prime.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public TFloatHash(int initialCapacity) {
        super(initialCapacity);
        this._hashingStrategy = this;
    }

    /**
     * Creates a new <code>TFloatHash</code> instance with a prime
     * value at or near the specified capacity and load factor.
     *
     * @param initialCapacity used to find a prime capacity for the table.
     * @param loadFactor      used to calculate the threshold over which
     *                        rehashing takes place.
     */
    public TFloatHash(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
        this._hashingStrategy = this;
    }

    /**
     * Creates a new <code>TFloatHash</code> instance with the default
     * capacity and load factor.
     *
     * @param strategy used to compute hash codes and to compare keys.
     */
    public TFloatHash(TFloatHashingStrategy strategy) {
        super();
        this._hashingStrategy = strategy;
    }

    /**
     * Creates a new <code>TFloatHash</code> instance whose capacity
     * is the next highest prime above <tt>initialCapacity + 1</tt>
     * unless that value is already prime.
     *
     * @param initialCapacity an <code>int</code> value
     * @param strategy        used to compute hash codes and to compare keys.
     */
    public TFloatHash(int initialCapacity, TFloatHashingStrategy strategy) {
        super(initialCapacity);
        this._hashingStrategy = strategy;
    }

    /**
     * Creates a new <code>TFloatHash</code> instance with a prime
     * value at or near the specified capacity and load factor.
     *
     * @param initialCapacity used to find a prime capacity for the table.
     * @param loadFactor      used to calculate the threshold over which
     *                        rehashing takes place.
     * @param strategy        used to compute hash codes and to compare keys.
     */
    public TFloatHash(int initialCapacity, float loadFactor, TFloatHashingStrategy strategy) {
        super(initialCapacity, loadFactor);
        this._hashingStrategy = strategy;
    }

    /**
     * @return a deep clone of this collection
     */
    public Object clone() {
        TFloatHash h = (TFloatHash) super.clone();
        h._set = (float[]) this._set.clone();
        return h;
    }

    /**
     * initializes the hashtable to a prime capacity which is at least
     * <tt>initialCapacity + 1</tt>.
     *
     * @param initialCapacity an <code>int</code> value
     * @return the actual capacity chosen
     */
    protected int setUp(int initialCapacity) {
        int capacity;

        capacity = super.setUp(initialCapacity);
        _set = new float[capacity];
        return capacity;
    }

    /**
     * Searches the set for <tt>val</tt>
     *
     * @param val an <code>float</code> value
     * @return a <code>boolean</code> value
     */
    public boolean contains(float val) {
        return index(val) >= 0;
    }

    /**
     * Executes <tt>procedure</tt> for each element in the set.
     *
     * @param procedure a <code>TObjectProcedure</code> value
     * @return false if the loop over the set terminated because
     *         the procedure returned false for some value.
     */
    public boolean forEach(TFloatProcedure procedure) {
        byte[] states = _states;
        float[] set = _set;
        for (int i = set.length; i-- > 0;) {
            if (states[i] == FULL && !procedure.execute(set[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Releases the element currently stored at <tt>index</tt>.
     *
     * @param index an <code>int</code> value
     */
    protected void removeAt(int index) {
        _set[index] = (float) 0;
        super.removeAt(index);
    }

    /**
     * Locates the index of <tt>val</tt>.
     *
     * @param val an <code>float</code> value
     * @return the index of <tt>val</tt> or -1 if it isn't in the set.
     */
    protected int index(float val) {
        int hash, probe, index, length;

        final byte[] states = _states;
        final float[] set = _set;
        length = states.length;
        hash = _hashingStrategy.computeHashCode(val) & 0x7fffffff;
        index = hash % length;

        if (states[index] != FREE &&
                (states[index] == REMOVED || set[index] != val)) {
            // see Knuth, p. 529
            probe = 1 + (hash % (length - 2));

            do {
                index -= probe;
                if (index < 0) {
                    index += length;
                }
            } while (states[index] != FREE &&
                    (states[index] == REMOVED || set[index] != val));
        }

        return states[index] == FREE ? -1 : index;
    }

    /**
     * Locates the index at which <tt>val</tt> can be inserted.  if
     * there is already a value equal()ing <tt>val</tt> in the set,
     * returns that value as a negative integer.
     *
     * @param val an <code>float</code> value
     * @return an <code>int</code> value
     */
    protected int insertionIndex(float val) {
        int hash, probe, index, length;

        final byte[] states = _states;
        final float[] set = _set;
        length = states.length;
        hash = _hashingStrategy.computeHashCode(val) & 0x7fffffff;
        index = hash % length;

        if (states[index] == FREE) {
            return index;       // empty, all done
        } else if (states[index] == FULL && set[index] == val) {
            return -index - 1;   // already stored
        } else {                // already FULL or REMOVED, must probe
            // compute the double hash
            probe = 1 + (hash % (length - 2));

            // if the slot we landed on is FULL (but not removed), probe
            // until we find an empty slot, a REMOVED slot, or an element
            // equal to the one we are trying to insert.
            // finding an empty slot means that the value is not present
            // and that we should use that slot as the insertion point;
            // finding a REMOVED slot means that we need to keep searching,
            // however we want to remember the offset of that REMOVED slot
            // so we can reuse it in case a "new" insertion (i.e. not an update)
            // is possible.
            // finding a matching value means that we've found that our desired
            // key is already in the table

            if (states[index] != REMOVED) {
                // starting at the natural offset, probe until we find an
                // offset that isn't full.
                do {
                    index -= probe;
                    if (index < 0) {
                        index += length;
                    }
                } while (states[index] == FULL && set[index] != val);
            }

            // if the index we found was removed: continue probing until we
            // locate a free location or an element which equal()s the
            // one we have.
            if (states[index] == REMOVED) {
                int firstRemoved = index;
                while (states[index] != FREE &&
                        (states[index] == REMOVED || set[index] != val)) {
                    index -= probe;
                    if (index < 0) {
                        index += length;
                    }
                }
                return states[index] == FULL ? -index - 1 : firstRemoved;
            }
            // if it's full, the key is already stored
            return states[index] == FULL ? -index - 1 : index;
        }
    }

    /**
     * Default implementation of TFloatHashingStrategy:
     * delegates hashing to HashFunctions.hash(float).
     *
     * @param val the value to hash
     * @return the hashcode.
     */
    public final int computeHashCode(float val) {
        return HashFunctions.hash(val);
    }
} // TFloatHash
