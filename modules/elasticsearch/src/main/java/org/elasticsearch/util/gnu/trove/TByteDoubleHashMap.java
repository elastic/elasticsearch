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
 * An open addressed Map implementation for byte keys and double values.
 * <p/>
 * Created: Sun Nov  4 08:52:45 2001
 *
 * @author Eric D. Friedman
 */
public class TByteDoubleHashMap extends TByteHash implements Externalizable {
    static final long serialVersionUID = 1L;

    private final TByteDoubleProcedure PUT_ALL_PROC = new TByteDoubleProcedure() {
        public boolean execute(byte key, double value) {
            put(key, value);
            return true;
        }
    };


    /**
     * the values of the map
     */
    protected transient double[] _values;

    /**
     * Creates a new <code>TByteDoubleHashMap</code> instance with the default
     * capacity and load factor.
     */
    public TByteDoubleHashMap() {
        super();
    }

    /**
     * Creates a new <code>TByteDoubleHashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public TByteDoubleHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Creates a new <code>TByteDoubleHashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the specified load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     */
    public TByteDoubleHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    /**
     * Creates a new <code>TByteDoubleHashMap</code> instance with the default
     * capacity and load factor.
     *
     * @param strategy used to compute hash codes and to compare keys.
     */
    public TByteDoubleHashMap(TByteHashingStrategy strategy) {
        super(strategy);
    }

    /**
     * Creates a new <code>TByteDoubleHashMap</code> instance whose capacity
     * is the next highest prime above <tt>initialCapacity + 1</tt>
     * unless that value is already prime.
     *
     * @param initialCapacity an <code>int</code> value
     * @param strategy        used to compute hash codes and to compare keys.
     */
    public TByteDoubleHashMap(int initialCapacity, TByteHashingStrategy strategy) {
        super(initialCapacity, strategy);
    }

    /**
     * Creates a new <code>TByteDoubleHashMap</code> instance with a prime
     * value at or near the specified capacity and load factor.
     *
     * @param initialCapacity used to find a prime capacity for the table.
     * @param loadFactor      used to calculate the threshold over which
     *                        rehashing takes place.
     * @param strategy        used to compute hash codes and to compare keys.
     */
    public TByteDoubleHashMap(int initialCapacity, float loadFactor, TByteHashingStrategy strategy) {
        super(initialCapacity, loadFactor, strategy);
    }

    /**
     * @return a deep clone of this collection
     */
    public Object clone() {
        TByteDoubleHashMap m = (TByteDoubleHashMap) super.clone();
        m._values = (double[]) this._values.clone();
        return m;
    }

    /**
     * @return a TByteDoubleIterator with access to this map's keys and values
     */
    public TByteDoubleIterator iterator() {
        return new TByteDoubleIterator(this);
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
        _values = new double[capacity];
        return capacity;
    }

    /**
     * Inserts a key/value pair into the map.
     *
     * @param key   an <code>byte</code> value
     * @param value an <code>double</code> value
     * @return the previous value associated with <tt>key</tt>,
     *         or (byte)0 if none was found.
     */
    public double put(byte key, double value) {
        int index = insertionIndex(key);
        return doPut(key, value, index);
    }

    /**
     * Inserts a key/value pair into the map if the specified key is not already
     * associated with a value.
     *
     * @param key   an <code>byte</code> value
     * @param value an <code>double</code> value
     * @return the previous value associated with <tt>key</tt>,
     *         or (byte)0 if none was found.
     */
    public double putIfAbsent(byte key, double value) {
        int index = insertionIndex(key);
        if (index < 0)
            return _values[-index - 1];
        return doPut(key, value, index);
    }

    private double doPut(byte key, double value, int index) {
        byte previousState;
        double previous = (double) 0;
        boolean isNewMapping = true;
        if (index < 0) {
            index = -index - 1;
            previous = _values[index];
            isNewMapping = false;
        }
        previousState = _states[index];
        _set[index] = key;
        _states[index] = FULL;
        _values[index] = value;
        if (isNewMapping) {
            postInsertHook(previousState == FREE);
        }

        return previous;
    }


    /**
     * Put all the entries from the given map into this map.
     *
     * @param map The map from which entries will be obtained to put into this map.
     */
    public void putAll(TByteDoubleHashMap map) {
        map.forEachEntry(PUT_ALL_PROC);
    }


    /**
     * rehashes the map to the new capacity.
     *
     * @param newCapacity an <code>int</code> value
     */
    protected void rehash(int newCapacity) {
        int oldCapacity = _set.length;
        byte oldKeys[] = _set;
        double oldVals[] = _values;
        byte oldStates[] = _states;

        _set = new byte[newCapacity];
        _values = new double[newCapacity];
        _states = new byte[newCapacity];

        for (int i = oldCapacity; i-- > 0;) {
            if (oldStates[i] == FULL) {
                byte o = oldKeys[i];
                int index = insertionIndex(o);
                _set[index] = o;
                _values[index] = oldVals[i];
                _states[index] = FULL;
            }
        }
    }

    /**
     * retrieves the value for <tt>key</tt>
     *
     * @param key an <code>byte</code> value
     * @return the value of <tt>key</tt> or (byte)0 if no such mapping exists.
     */
    public double get(byte key) {
        int index = index(key);
        return index < 0 ? (double) 0 : _values[index];
    }

    /**
     * Empties the map.
     */
    public void clear() {
        super.clear();
        byte[] keys = _set;
        double[] vals = _values;
        byte[] states = _states;

        Arrays.fill(_set, 0, _set.length, (byte) 0);
        Arrays.fill(_values, 0, _values.length, (double) 0);
        Arrays.fill(_states, 0, _states.length, FREE);
    }

    /**
     * Deletes a key/value pair from the map.
     *
     * @param key an <code>byte</code> value
     * @return an <code>double</code> value, or (byte)0 if no mapping for key exists
     */
    public double remove(byte key) {
        double prev = (double) 0;
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
        if (!(other instanceof TByteDoubleHashMap)) {
            return false;
        }
        TByteDoubleHashMap that = (TByteDoubleHashMap) other;
        if (that.size() != this.size()) {
            return false;
        }
        return forEachEntry(new EqProcedure(that));
    }

    public int hashCode() {
        HashProcedure p = new HashProcedure();
        forEachEntry(p);
        return p.getHashCode();
    }

    private final class HashProcedure implements TByteDoubleProcedure {
        private int h = 0;

        public int getHashCode() {
            return h;
        }

        public final boolean execute(byte key, double value) {
            h += (_hashingStrategy.computeHashCode(key) ^ HashFunctions.hash(value));
            return true;
        }
    }

    private static final class EqProcedure implements TByteDoubleProcedure {
        private final TByteDoubleHashMap _otherMap;

        EqProcedure(TByteDoubleHashMap otherMap) {
            _otherMap = otherMap;
        }

        public final boolean execute(byte key, double value) {
            int index = _otherMap.index(key);
            if (index >= 0 && eq(value, _otherMap.get(key))) {
                return true;
            }
            return false;
        }

        /**
         * Compare two doubles for equality.
         */
        private final boolean eq(double v1, double v2) {
            return v1 == v2;
        }

    }

    /**
     * removes the mapping at <tt>index</tt> from the map.
     *
     * @param index an <code>int</code> value
     */
    protected void removeAt(int index) {
        _values[index] = (double) 0;
        super.removeAt(index);  // clear key, state; adjust size
    }

    /**
     * Returns the values of the map.
     *
     * @return a <code>Collection</code> value
     */
    public double[] getValues() {
        double[] vals = new double[size()];
        double[] v = _values;
        byte[] states = _states;

        for (int i = v.length, j = 0; i-- > 0;) {
            if (states[i] == FULL) {
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
    public byte[] keys() {
        byte[] keys = new byte[size()];
        byte[] k = _set;
        byte[] states = _states;

        for (int i = k.length, j = 0; i-- > 0;) {
            if (states[i] == FULL) {
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
     *          same type is allocated for this purpose.
     * @return a <code>Set</code> value
     */
    public byte[] keys(byte[] a) {
        int size = size();
        if (a.length < size) {
            a = (byte[]) java.lang.reflect.Array.newInstance(
                    a.getClass().getComponentType(), size);
        }

        byte[] k = (byte[]) _set;
        byte[] states = _states;

        for (int i = k.length, j = 0; i-- > 0;) {
            if (states[i] == FULL) {
                a[j++] = k[i];
            }
        }
        return a;
    }

    /**
     * checks for the presence of <tt>val</tt> in the values of the map.
     *
     * @param val an <code>double</code> value
     * @return a <code>boolean</code> value
     */
    public boolean containsValue(double val) {
        byte[] states = _states;
        double[] vals = _values;

        for (int i = vals.length; i-- > 0;) {
            if (states[i] == FULL && val == vals[i]) {
                return true;
            }
        }
        return false;
    }


    /**
     * checks for the present of <tt>key</tt> in the keys of the map.
     *
     * @param key an <code>byte</code> value
     * @return a <code>boolean</code> value
     */
    public boolean containsKey(byte key) {
        return contains(key);
    }

    /**
     * Executes <tt>procedure</tt> for each key in the map.
     *
     * @param procedure a <code>TByteProcedure</code> value
     * @return false if the loop over the keys terminated because
     *         the procedure returned false for some key.
     */
    public boolean forEachKey(TByteProcedure procedure) {
        return forEach(procedure);
    }

    /**
     * Executes <tt>procedure</tt> for each value in the map.
     *
     * @param procedure a <code>TDoubleProcedure</code> value
     * @return false if the loop over the values terminated because
     *         the procedure returned false for some value.
     */
    public boolean forEachValue(TDoubleProcedure procedure) {
        byte[] states = _states;
        double[] values = _values;
        for (int i = values.length; i-- > 0;) {
            if (states[i] == FULL && !procedure.execute(values[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Executes <tt>procedure</tt> for each key/value entry in the
     * map.
     *
     * @param procedure a <code>TOByteDoubleProcedure</code> value
     * @return false if the loop over the entries terminated because
     *         the procedure returned false for some entry.
     */
    public boolean forEachEntry(TByteDoubleProcedure procedure) {
        byte[] states = _states;
        byte[] keys = _set;
        double[] values = _values;
        for (int i = keys.length; i-- > 0;) {
            if (states[i] == FULL && !procedure.execute(keys[i], values[i])) {
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
    public boolean retainEntries(TByteDoubleProcedure procedure) {
        boolean modified = false;
        byte[] states = _states;
        byte[] keys = _set;
        double[] values = _values;


        // Temporarily disable compaction. This is a fix for bug #1738760
        tempDisableAutoCompaction();
        try {
            for (int i = keys.length; i-- > 0;) {
                if (states[i] == FULL && !procedure.execute(keys[i], values[i])) {
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
     * @param function a <code>TDoubleFunction</code> value
     */
    public void transformValues(TDoubleFunction function) {
        byte[] states = _states;
        double[] values = _values;
        for (int i = values.length; i-- > 0;) {
            if (states[i] == FULL) {
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
    public boolean increment(byte key) {
        return adjustValue(key, (double) 1);
    }

    /**
     * Adjusts the primitive value mapped to key.
     *
     * @param key    the key of the value to increment
     * @param amount the amount to adjust the value by.
     * @return true if a mapping was found and modified.
     */
    public boolean adjustValue(byte key, double amount) {
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
    public double adjustOrPutValue(final byte key, final double adjust_amount, final double put_amount) {
        int index = insertionIndex(key);
        final boolean isNewMapping;
        final double newValue;
        if (index < 0) {
            index = -index - 1;
            newValue = (_values[index] += adjust_amount);
            isNewMapping = false;
        } else {
            newValue = (_values[index] = put_amount);
            isNewMapping = true;
        }

        byte previousState = _states[index];
        _set[index] = key;
        _states[index] = FULL;

        if (isNewMapping) {
            postInsertHook(previousState == FREE);
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
            byte key = in.readByte();
            double val = in.readDouble();
            put(key, val);
        }
    }

    public String toString() {
        final StringBuilder buf = new StringBuilder("{");
        forEachEntry(new TByteDoubleProcedure() {
            private boolean first = true;

            public boolean execute(byte key, double value) {
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
} // TByteDoubleHashMap
