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

/**
 * The base class for hashtables of primitive values.  Since there is
 * no notion of object equality for primitives, it isn't possible to
 * use a `REMOVED' object to track deletions in an open-addressed table.
 * So, we have to resort to using a parallel `bookkeeping' array of bytes,
 * in which flags can be set to indicate that a particular slot in the
 * hash table is FREE, FULL, or REMOVED.
 * <p/>
 * Created: Fri Jan 11 18:55:16 2002
 *
 * @author Eric D. Friedman
 * @version $Id: TPrimitiveHash.java,v 1.5 2008/10/08 16:39:10 robeden Exp $
 */

abstract public class TPrimitiveHash extends THash {
    /**
     * flags indicating whether each position in the hash is
     * FREE, FULL, or REMOVED
     */
    protected transient byte[] _states;

    /* constants used for state flags */

    /**
     * flag indicating that a slot in the hashtable is available
     */
    protected static final byte FREE = 0;

    /**
     * flag indicating that a slot in the hashtable is occupied
     */
    protected static final byte FULL = 1;

    /**
     * flag indicating that the value of a slot in the hashtable
     * was deleted
     */
    protected static final byte REMOVED = 2;

    /**
     * Creates a new <code>THash</code> instance with the default
     * capacity and load factor.
     */
    public TPrimitiveHash() {
        super();
    }

    /**
     * Creates a new <code>TPrimitiveHash</code> instance with a prime
     * capacity at or near the specified capacity and with the default
     * load factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public TPrimitiveHash(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Creates a new <code>TPrimitiveHash</code> instance with a prime
     * capacity at or near the minimum needed to hold
     * <tt>initialCapacity<tt> elements with load factor
     * <tt>loadFactor</tt> without triggering a rehash.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     */
    public TPrimitiveHash(int initialCapacity, float loadFactor) {
        super();
        _loadFactor = loadFactor;
        setUp(HashFunctions.fastCeil(initialCapacity / loadFactor));
    }

    public Object clone() {
        TPrimitiveHash h = (TPrimitiveHash) super.clone();
        h._states = (byte[]) this._states.clone();
        return h;
    }

    /**
     * Returns the capacity of the hash table.  This is the true
     * physical capacity, without adjusting for the load factor.
     *
     * @return the physical capacity of the hash table.
     */
    protected int capacity() {
        return _states.length;
    }

    /**
     * Delete the record at <tt>index</tt>.
     *
     * @param index an <code>int</code> value
     */
    protected void removeAt(int index) {
        _states[index] = REMOVED;
        super.removeAt(index);
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
        _states = new byte[capacity];
        return capacity;
    }
} // TPrimitiveHash
