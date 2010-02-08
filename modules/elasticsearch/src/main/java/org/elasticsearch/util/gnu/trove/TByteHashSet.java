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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;


//////////////////////////////////////////////////
// THIS IS A GENERATED CLASS. DO NOT HAND EDIT! //
//////////////////////////////////////////////////


/**
 * An open addressed set implementation for byte primitives.
 *
 * @author Eric D. Friedman
 * @author Rob Eden
 */

public class TByteHashSet extends TByteHash implements Externalizable {
    static final long serialVersionUID = 1L;

    /**
     * Creates a new <code>TByteHashSet</code> instance with the default
     * capacity and load factor.
     */
    public TByteHashSet() {
        super();
    }

    /**
     * Creates a new <code>TByteHashSet</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public TByteHashSet(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Creates a new <code>TByteHashSet</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the specified load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     */
    public TByteHashSet(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    /**
     * Creates a new <code>TByteHashSet</code> instance containing the
     * elements of <tt>array</tt>.
     *
     * @param array an array of <code>byte</code> primitives
     */
    public TByteHashSet(byte[] array) {
        this(array.length);
        addAll(array);
    }

    /**
     * Creates a new <code>TByteHash</code> instance with the default
     * capacity and load factor.
     *
     * @param strategy used to compute hash codes and to compare keys.
     */
    public TByteHashSet(TByteHashingStrategy strategy) {
        super(strategy);
    }

    /**
     * Creates a new <code>TByteHash</code> instance whose capacity
     * is the next highest prime above <tt>initialCapacity + 1</tt>
     * unless that value is already prime.
     *
     * @param initialCapacity an <code>int</code> value
     * @param strategy        used to compute hash codes and to compare keys.
     */
    public TByteHashSet(int initialCapacity, TByteHashingStrategy strategy) {
        super(initialCapacity, strategy);
    }

    /**
     * Creates a new <code>TByteHash</code> instance with a prime
     * value at or near the specified capacity and load factor.
     *
     * @param initialCapacity used to find a prime capacity for the table.
     * @param loadFactor      used to calculate the threshold over which
     *                        rehashing takes place.
     * @param strategy        used to compute hash codes and to compare keys.
     */
    public TByteHashSet(int initialCapacity, float loadFactor, TByteHashingStrategy strategy) {
        super(initialCapacity, loadFactor, strategy);
    }

    /**
     * Creates a new <code>TByteHashSet</code> instance containing the
     * elements of <tt>array</tt>.
     *
     * @param array    an array of <code>byte</code> primitives
     * @param strategy used to compute hash codes and to compare keys.
     */
    public TByteHashSet(byte[] array, TByteHashingStrategy strategy) {
        this(array.length, strategy);
        addAll(array);
    }

    /**
     * @return a TByteIterator with access to the values in this set
     */
    public TByteIterator iterator() {
        return new TByteIterator(this);
    }

    /**
     * Inserts a value into the set.
     *
     * @param val an <code>byte</code> value
     * @return true if the set was modified by the add operation
     */
    public boolean add(byte val) {
        int index = insertionIndex(val);

        if (index < 0) {
            return false;       // already present in set, nothing to add
        }

        byte previousState = _states[index];
        _set[index] = val;
        _states[index] = FULL;
        postInsertHook(previousState == FREE);

        return true;            // yes, we added something
    }

    /**
     * Expands the set to accommodate new values.
     *
     * @param newCapacity an <code>int</code> value
     */
    protected void rehash(int newCapacity) {
        int oldCapacity = _set.length;
        byte oldSet[] = _set;
        byte oldStates[] = _states;

        _set = new byte[newCapacity];
        _states = new byte[newCapacity];

        for (int i = oldCapacity; i-- > 0;) {
            if (oldStates[i] == FULL) {
                byte o = oldSet[i];
                int index = insertionIndex(o);
                _set[index] = o;
                _states[index] = FULL;
            }
        }
    }

    /**
     * Returns a new array containing the values in the set.
     *
     * @return an <code>byte[]</code> value
     */
    public byte[] toArray() {
        byte[] result = new byte[size()];
        byte[] set = _set;
        byte[] states = _states;

        for (int i = states.length, j = 0; i-- > 0;) {
            if (states[i] == FULL) {
                result[j++] = set[i];
            }
        }
        return result;
    }

    /**
     * Empties the set.
     */
    public void clear() {
        super.clear();
        byte[] set = _set;
        byte[] states = _states;

        for (int i = set.length; i-- > 0;) {
            set[i] = (byte) 0;
            states[i] = FREE;
        }
    }

    /**
     * Compares this set with another set for equality of their stored
     * entries.
     *
     * @param other an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    public boolean equals(Object other) {
        if (!(other instanceof TByteHashSet)) {
            return false;
        }
        final TByteHashSet that = (TByteHashSet) other;
        if (that.size() != this.size()) {
            return false;
        }
        return forEach(new TByteProcedure() {
            public final boolean execute(byte value) {
                return that.contains(value);
            }
        });
    }

    public int hashCode() {
        HashProcedure p = new HashProcedure();
        forEach(p);
        return p.getHashCode();
    }

    private final class HashProcedure implements TByteProcedure {
        private int h = 0;

        public int getHashCode() {
            return h;
        }

        public final boolean execute(byte key) {
            h += _hashingStrategy.computeHashCode(key);
            return true;
        }
    }

    /**
     * Removes <tt>val</tt> from the set.
     *
     * @param val an <code>byte</code> value
     * @return true if the set was modified by the remove operation.
     */
    public boolean remove(byte val) {
        int index = index(val);
        if (index >= 0) {
            removeAt(index);
            return true;
        }
        return false;
    }

    /**
     * Tests the set to determine if all of the elements in
     * <tt>array</tt> are present.
     *
     * @param array an <code>array</code> of byte primitives.
     * @return true if all elements were present in the set.
     */
    public boolean containsAll(byte[] array) {
        for (int i = array.length; i-- > 0;) {
            if (!contains(array[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Adds all of the elements in <tt>array</tt> to the set.
     *
     * @param array an <code>array</code> of byte primitives.
     * @return true if the set was modified by the add all operation.
     */
    public boolean addAll(byte[] array) {
        boolean changed = false;
        for (int i = array.length; i-- > 0;) {
            if (add(array[i])) {
                changed = true;
            }
        }
        return changed;
    }

    /**
     * Removes all of the elements in <tt>array</tt> from the set.
     *
     * @param array an <code>array</code> of byte primitives.
     * @return true if the set was modified by the remove all operation.
     */
    public boolean removeAll(byte[] array) {
        boolean changed = false;
        for (int i = array.length; i-- > 0;) {
            if (remove(array[i])) {
                changed = true;
            }
        }
        return changed;
    }

    /**
     * Removes any values in the set which are not contained in
     * <tt>array</tt>.
     *
     * @param array an <code>array</code> of byte primitives.
     * @return true if the set was modified by the retain all operation
     */
    public boolean retainAll(byte[] array) {
        boolean changed = false;
        Arrays.sort(array);
        byte[] set = _set;
        byte[] states = _states;

        for (int i = set.length; i-- > 0;) {
            if (states[i] == FULL && (Arrays.binarySearch(array, set[i]) < 0)) {
                remove(set[i]);
                changed = true;
            }
        }
        return changed;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        // VERSION
        out.writeByte(0);

        // NUMBER OF ENTRIES
        out.writeInt(_size);

        // ENTRIES
        SerializationProcedure writeProcedure = new SerializationProcedure(out);
        if (!forEach(writeProcedure)) {
            throw writeProcedure.exception;
        }
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {

        // VERSION
        in.readByte();

        // NUMBER OF ENTRIES
        int size = in.readInt();

        // ENTRIES
        setUp(size);
        while (size-- > 0) {
            byte val = in.readByte();
            add(val);
        }
    }
} // TByteHashSet
