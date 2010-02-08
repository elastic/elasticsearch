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
 * An open addressed Map implementation for Object keys and byte values.
 * <p/>
 * Created: Sun Nov  4 08:52:45 2001
 *
 * @author Eric D. Friedman
 */
public class TObjectByteHashMap<K> extends TObjectHash<K> implements Externalizable {
    static final long serialVersionUID = 1L;

    private final TObjectByteProcedure<K> PUT_ALL_PROC = new TObjectByteProcedure<K>() {
        public boolean execute(K key, byte value) {
            put(key, value);
            return true;
        }
    };

    /**
     * the values of the map
     */
    protected transient byte[] _values;

    /**
     * Creates a new <code>TObjectByteHashMap</code> instance with the default
     * capacity and load factor.
     */
    public TObjectByteHashMap() {
        super();
    }

    /**
     * Creates a new <code>TObjectByteHashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public TObjectByteHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Creates a new <code>TObjectByteHashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the specified load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     */
    public TObjectByteHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    /**
     * Creates a new <code>TObjectByteHashMap</code> instance with the default
     * capacity and load factor.
     *
     * @param strategy used to compute hash codes and to compare keys.
     */
    public TObjectByteHashMap(TObjectHashingStrategy<K> strategy) {
        super(strategy);
    }

    /**
     * Creates a new <code>TObjectByteHashMap</code> instance whose capacity
     * is the next highest prime above <tt>initialCapacity + 1</tt>
     * unless that value is already prime.
     *
     * @param initialCapacity an <code>int</code> value
     * @param strategy        used to compute hash codes and to compare keys.
     */
    public TObjectByteHashMap(int initialCapacity, TObjectHashingStrategy<K> strategy) {
        super(initialCapacity, strategy);
    }

    /**
     * Creates a new <code>TObjectByteHashMap</code> instance with a prime
     * value at or near the specified capacity and load factor.
     *
     * @param initialCapacity used to find a prime capacity for the table.
     * @param loadFactor      used to calculate the threshold over which
     *                        rehashing takes place.
     * @param strategy        used to compute hash codes and to compare keys.
     */
    public TObjectByteHashMap(int initialCapacity, float loadFactor, TObjectHashingStrategy<K> strategy) {
        super(initialCapacity, loadFactor, strategy);
    }

    /**
     * @return an iterator over the entries in this map
     */
    public TObjectByteIterator<K> iterator() {
        return new TObjectByteIterator<K>(this);
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
        _values = new byte[capacity];
        return capacity;
    }

    /**
     * Inserts a key/value pair into the map.
     *
     * @param key   an <code>Object</code> value
     * @param value an <code>byte</code> value
     * @return the previous value associated with <tt>key</tt>,
     *         or (byte)0 if none was found.
     */
    public byte put(K key, byte value) {
        int index = insertionIndex(key);
        return doPut(key, value, index);
    }

    /**
     * Inserts a key/value pair into the map if the specified key is not already
     * associated with a value.
     *
     * @param key   an <code>Object</code> value
     * @param value an <code>byte</code> value
     * @return the previous value associated with <tt>key</tt>,
     *         or (byte)0 if none was found.
     */
    public byte putIfAbsent(K key, byte value) {
        int index = insertionIndex(key);
        if (index < 0)
            return _values[-index - 1];
        return doPut(key, value, index);
    }

    private byte doPut(K key, byte value, int index) {
        byte previous = (byte) 0;
        boolean isNewMapping = true;
        if (index < 0) {
            index = -index - 1;
            previous = _values[index];
            isNewMapping = false;
        }
        K oldKey = (K) _set[index];
        _set[index] = key;
        _values[index] = value;

        if (isNewMapping) {
            postInsertHook(oldKey == FREE);
        }
        return previous;
    }


    /**
     * Put all the entries from the given map into this map.
     *
     * @param map The map from which entries will be obtained to put into this map.
     */
    public void putAll(TObjectByteHashMap<K> map) {
        map.forEachEntry(PUT_ALL_PROC);
    }


    /**
     * rehashes the map to the new capacity.
     *
     * @param newCapacity an <code>int</code> value
     */
    protected void rehash(int newCapacity) {
        int oldCapacity = _set.length;
        K oldKeys[] = (K[]) _set;
        byte oldVals[] = _values;

        _set = new Object[newCapacity];
        Arrays.fill(_set, FREE);
        _values = new byte[newCapacity];

        for (int i = oldCapacity; i-- > 0;) {
            if (oldKeys[i] != FREE && oldKeys[i] != REMOVED) {
                K o = oldKeys[i];
                int index = insertionIndex(o);
                if (index < 0) {
                    throwObjectContractViolation(_set[(-index - 1)], o);
                }
                _set[index] = o;
                _values[index] = oldVals[i];
            }
        }
    }

    /**
     * retrieves the value for <tt>key</tt>
     *
     * @param key an <code>Object</code> value
     * @return the value of <tt>key</tt> or (byte)0 if no such mapping exists.
     */
    public byte get(K key) {
        int index = index(key);
        return index < 0 ? (byte) 0 : _values[index];
    }

    /**
     * Empties the map.
     */
    public void clear() {
        super.clear();
        Object[] keys = _set;
        byte[] vals = _values;

        Arrays.fill(_set, 0, _set.length, FREE);
        Arrays.fill(_values, 0, _values.length, (byte) 0);
    }

    /**
     * Deletes a key/value pair from the map.
     *
     * @param key an <code>Object</code> value
     * @return an <code>byte</code> value or (byte)0 if no such mapping exists.
     */
    public byte remove(K key) {
        byte prev = (byte) 0;
        int index = index(key);
        if (index >= 0) {
            prev = _values[index];
            removeAt(index);    // clear key,state; adjust size
        }
        return prev;
    }

    /**
     * Compares this map with another map for equality of their stored
     * entries.
     *
     * @param other an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    public boolean equals(Object other) {
        if (!(other instanceof TObjectByteHashMap)) {
            return false;
        }
        TObjectByteHashMap that = (TObjectByteHashMap) other;
        if (that.size() != this.size()) {
            return false;
        }
        return forEachEntry(new EqProcedure(that));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TObjectByteHashMap<K> clone() {
        TObjectByteHashMap<K> clone = (TObjectByteHashMap<K>) super.clone();
        clone._values = new byte[_values.length];
        System.arraycopy(_values, 0, clone._values, 0, clone._values.length);

        return clone;
    }


    private static final class EqProcedure implements TObjectByteProcedure {
        private final TObjectByteHashMap _otherMap;

        EqProcedure(TObjectByteHashMap otherMap) {
            _otherMap = otherMap;
        }

        public final boolean execute(Object key, byte value) {
            int index = _otherMap.index(key);
            if (index >= 0 && eq(value, _otherMap.get(key))) {
                return true;
            }
            return false;
        }

        /**
         * Compare two bytes for equality.
         */
        private final boolean eq(byte v1, byte v2) {
            return v1 == v2;
        }

    }

    /**
     * removes the mapping at <tt>index</tt> from the map.
     *
     * @param index an <code>int</code> value
     */
    protected void removeAt(int index) {
        _values[index] = 0;
        super.removeAt(index);  // clear key, state; adjust size
    }

    /**
     * Returns the values of the map.
     *
     * @return a <code>Collection</code> value
     */
    public byte[] getValues() {
        byte[] vals = new byte[size()];
        byte[] v = _values;
        Object[] keys = _set;

        for (int i = v.length, j = 0; i-- > 0;) {
            if (keys[i] != FREE && keys[i] != REMOVED) {
                vals[j++] = v[i];
            }
        }
        return vals;
    }

    /**
     * returns the keys of the map.
     *
     * @return a <code>Set</code> value
     */
    public Object[] keys() {
        Object[] keys = new Object[size()];
        K[] k = (K[]) _set;

        for (int i = k.length, j = 0; i-- > 0;) {
            if (k[i] != FREE && k[i] != REMOVED) {
                keys[j++] = k[i];
            }
        }
        return keys;
    }

    /**
     * returns the keys of the map.
     *
     * @param a the array into which the elements of the list are to
     *          be stored, if it is big enough; otherwise, a new array of the
     *          same runtime type is allocated for this purpose.
     * @return a <code>Set</code> value
     */
    public K[] keys(K[] a) {
        int size = size();
        if (a.length < size) {
            a = (K[]) java.lang.reflect.Array.newInstance(
                    a.getClass().getComponentType(), size);
        }

        K[] k = (K[]) _set;

        for (int i = k.length, j = 0; i-- > 0;) {
            if (k[i] != FREE && k[i] != REMOVED) {
                a[j++] = k[i];
            }
        }
        return a;
    }

    /**
     * checks for the presence of <tt>val</tt> in the values of the map.
     *
     * @param val an <code>byte</code> value
     * @return a <code>boolean</code> value
     */
    public boolean containsValue(byte val) {
        Object[] keys = _set;
        byte[] vals = _values;

        for (int i = vals.length; i-- > 0;) {
            if (keys[i] != FREE && keys[i] != REMOVED && val == vals[i]) {
                return true;
            }
        }
        return false;
    }


    /**
     * checks for the present of <tt>key</tt> in the keys of the map.
     *
     * @param key an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    public boolean containsKey(K key) {
        return contains(key);
    }

    /**
     * Executes <tt>procedure</tt> for each key in the map.
     *
     * @param procedure a <code>TObjectProcedure</code> value
     * @return false if the loop over the keys terminated because
     *         the procedure returned false for some key.
     */
    public boolean forEachKey(TObjectProcedure<K> procedure) {
        return forEach(procedure);
    }

    /**
     * Executes <tt>procedure</tt> for each value in the map.
     *
     * @param procedure a <code>TByteProcedure</code> value
     * @return false if the loop over the values terminated because
     *         the procedure returned false for some value.
     */
    public boolean forEachValue(TByteProcedure procedure) {
        Object[] keys = _set;
        byte[] values = _values;
        for (int i = values.length; i-- > 0;) {
            if (keys[i] != FREE && keys[i] != REMOVED
                    && !procedure.execute(values[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Executes <tt>procedure</tt> for each key/value entry in the
     * map.
     *
     * @param procedure a <code>TOObjectByteProcedure</code> value
     * @return false if the loop over the entries terminated because
     *         the procedure returned false for some entry.
     */
    public boolean forEachEntry(TObjectByteProcedure<K> procedure) {
        K[] keys = (K[]) _set;
        byte[] values = _values;
        for (int i = keys.length; i-- > 0;) {
            if (keys[i] != FREE
                    && keys[i] != REMOVED
                    && !procedure.execute(keys[i], values[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Retains only those entries in the map for which the procedure
     * returns a true value.
     *
     * @param procedure determines which entries to keep
     * @return true if the map was modified.
     */
    public boolean retainEntries(TObjectByteProcedure<K> procedure) {
        boolean modified = false;
        K[] keys = (K[]) _set;
        byte[] values = _values;

        // Temporarily disable compaction. This is a fix for bug #1738760
        tempDisableAutoCompaction();
        try {
            for (int i = keys.length; i-- > 0;) {
                if (keys[i] != FREE
                        && keys[i] != REMOVED
                        && !procedure.execute(keys[i], values[i])) {
                    removeAt(i);
                    modified = true;
                }
            }
        }
        finally {
            reenableAutoCompaction(true);
        }

        return modified;
    }

    /**
     * Transform the values in this map using <tt>function</tt>.
     *
     * @param function a <code>TByteFunction</code> value
     */
    public void transformValues(TByteFunction function) {
        Object[] keys = _set;
        byte[] values = _values;
        for (int i = values.length; i-- > 0;) {
            if (keys[i] != null && keys[i] != REMOVED) {
                values[i] = function.execute(values[i]);
            }
        }
    }

    /**
     * Increments the primitive value mapped to key by 1
     *
     * @param key the key of the value to increment
     * @return true if a mapping was found and modified.
     */
    public boolean increment(K key) {
        return adjustValue(key, (byte) 1);
    }

    /**
     * Adjusts the primitive value mapped to key.
     *
     * @param key    the key of the value to increment
     * @param amount the amount to adjust the value by.
     * @return true if a mapping was found and modified.
     */
    public boolean adjustValue(K key, byte amount) {
        int index = index(key);
        if (index < 0) {
            return false;
        } else {
            _values[index] += amount;
            return true;
        }
    }

    /**
     * Adjusts the primitive value mapped to the key if the key is present in the map.
     * Otherwise, the <tt>initial_value</tt> is put in the map.
     *
     * @param key           the key of the value to increment
     * @param adjust_amount the amount to adjust the value by
     * @param put_amount    the value put into the map if the key is not initial present
     * @return the value present in the map after the adjustment or put operation
     * @since 2.0b1
     */
    public byte adjustOrPutValue(final K key, final byte adjust_amount, final byte put_amount) {
        int index = insertionIndex(key);
        final boolean isNewMapping;
        final byte newValue;
        if (index < 0) {
            index = -index - 1;
            newValue = (_values[index] += adjust_amount);
            isNewMapping = false;
        } else {
            newValue = (_values[index] = put_amount);
            isNewMapping = true;
        }

        K oldKey = (K) _set[index];
        _set[index] = key;

        if (isNewMapping) {
            postInsertHook(oldKey == FREE);
        }

        return newValue;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        // VERSION
        out.writeByte(0);

        // NUMBER OF ENTRIES
        out.writeInt(_size);

        // ENTRIES
        SerializationProcedure writeProcedure = new SerializationProcedure(out);
        if (!forEachEntry(writeProcedure)) {
            throw writeProcedure.exception;
        }
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {

        // VERSION
        in.readByte();

        // NUMBER OF ENTRIES
        int size = in.readInt();
        setUp(size);

        // ENTRIES
        while (size-- > 0) {
            K key = (K) in.readObject();
            byte val = in.readByte();
            put(key, val);
        }
    }


    public String toString() {
        final StringBuilder buf = new StringBuilder("{");
        forEachEntry(new TObjectByteProcedure<K>() {
            private boolean first = true;

            public boolean execute(K key, byte value) {
                if (first) first = false;
                else buf.append(",");

                buf.append(key);
                buf.append("=");
                buf.append(value);
                return true;
            }
        });
        buf.append("}");
        return buf.toString();
    }
} // TObjectByteHashMap
