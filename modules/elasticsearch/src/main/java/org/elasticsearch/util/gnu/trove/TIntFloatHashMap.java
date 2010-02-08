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
 * An open addressed Map implementation for int keys and float values.
 * <p/>
 * Created: Sun Nov  4 08:52:45 2001
 *
 * @author Eric D. Friedman
 */
public class TIntFloatHashMap extends TIntHash implements Externalizable {
    static final long serialVersionUID = 1L;

    private final TIntFloatProcedure PUT_ALL_PROC = new TIntFloatProcedure() {
        public boolean execute(int key, float value) {
            put(key, value);
            return true;
        }
    };


    /**
     * the values of the map
     */
    protected transient float[] _values;

    /**
     * Creates a new <code>TIntFloatHashMap</code> instance with the default
     * capacity and load factor.
     */
    public TIntFloatHashMap() {
        super();
    }

    /**
     * Creates a new <code>TIntFloatHashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public TIntFloatHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Creates a new <code>TIntFloatHashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the specified load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     */
    public TIntFloatHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    /**
     * Creates a new <code>TIntFloatHashMap</code> instance with the default
     * capacity and load factor.
     *
     * @param strategy used to compute hash codes and to compare keys.
     */
    public TIntFloatHashMap(TIntHashingStrategy strategy) {
        super(strategy);
    }

    /**
     * Creates a new <code>TIntFloatHashMap</code> instance whose capacity
     * is the next highest prime above <tt>initialCapacity + 1</tt>
     * unless that value is already prime.
     *
     * @param initialCapacity an <code>int</code> value
     * @param strategy        used to compute hash codes and to compare keys.
     */
    public TIntFloatHashMap(int initialCapacity, TIntHashingStrategy strategy) {
        super(initialCapacity, strategy);
    }

    /**
     * Creates a new <code>TIntFloatHashMap</code> instance with a prime
     * value at or near the specified capacity and load factor.
     *
     * @param initialCapacity used to find a prime capacity for the table.
     * @param loadFactor      used to calculate the threshold over which
     *                        rehashing takes place.
     * @param strategy        used to compute hash codes and to compare keys.
     */
    public TIntFloatHashMap(int initialCapacity, float loadFactor, TIntHashingStrategy strategy) {
        super(initialCapacity, loadFactor, strategy);
    }

    /**
     * @return a deep clone of this collection
     */
    public Object clone() {
        TIntFloatHashMap m = (TIntFloatHashMap) super.clone();
        m._values = (float[]) this._values.clone();
        return m;
    }

    /**
     * @return a TIntFloatIterator with access to this map's keys and values
     */
    public TIntFloatIterator iterator() {
        return new TIntFloatIterator(this);
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
        _values = new float[capacity];
        return capacity;
    }

    /**
     * Inserts a key/value pair into the map.
     *
     * @param key   an <code>int</code> value
     * @param value an <code>float</code> value
     * @return the previous value associated with <tt>key</tt>,
     *         or (int)0 if none was found.
     */
    public float put(int key, float value) {
        int index = insertionIndex(key);
        return doPut(key, value, index);
    }

    /**
     * Inserts a key/value pair into the map if the specified key is not already
     * associated with a value.
     *
     * @param key   an <code>int</code> value
     * @param value an <code>float</code> value
     * @return the previous value associated with <tt>key</tt>,
     *         or (int)0 if none was found.
     */
    public float putIfAbsent(int key, float value) {
        int index = insertionIndex(key);
        if (index < 0)
            return _values[-index - 1];
        return doPut(key, value, index);
    }

    private float doPut(int key, float value, int index) {
        byte previousState;
        float previous = (float) 0;
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
    public void putAll(TIntFloatHashMap map) {
        map.forEachEntry(PUT_ALL_PROC);
    }


    /**
     * rehashes the map to the new capacity.
     *
     * @param newCapacity an <code>int</code> value
     */
    protected void rehash(int newCapacity) {
        int oldCapacity = _set.length;
        int oldKeys[] = _set;
        float oldVals[] = _values;
        byte oldStates[] = _states;

        _set = new int[newCapacity];
        _values = new float[newCapacity];
        _states = new byte[newCapacity];

        for (int i = oldCapacity; i-- > 0;) {
            if (oldStates[i] == FULL) {
                int o = oldKeys[i];
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
     * @param key an <code>int</code> value
     * @return the value of <tt>key</tt> or (int)0 if no such mapping exists.
     */
    public float get(int key) {
        int index = index(key);
        return index < 0 ? (float) 0 : _values[index];
    }

    /**
     * Empties the map.
     */
    public void clear() {
        super.clear();
        int[] keys = _set;
        float[] vals = _values;
        byte[] states = _states;

        Arrays.fill(_set, 0, _set.length, (int) 0);
        Arrays.fill(_values, 0, _values.length, (float) 0);
        Arrays.fill(_states, 0, _states.length, FREE);
    }

    /**
     * Deletes a key/value pair from the map.
     *
     * @param key an <code>int</code> value
     * @return an <code>float</code> value, or (int)0 if no mapping for key exists
     */
    public float remove(int key) {
        float prev = (float) 0;
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
        if (!(other instanceof TIntFloatHashMap)) {
            return false;
        }
        TIntFloatHashMap that = (TIntFloatHashMap) other;
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

    private final class HashProcedure implements TIntFloatProcedure {
        private int h = 0;

        public int getHashCode() {
            return h;
        }

        public final boolean execute(int key, float value) {
            h += (_hashingStrategy.computeHashCode(key) ^ HashFunctions.hash(value));
            return true;
        }
    }

    private static final class EqProcedure implements TIntFloatProcedure {
        private final TIntFloatHashMap _otherMap;

        EqProcedure(TIntFloatHashMap otherMap) {
            _otherMap = otherMap;
        }

        public final boolean execute(int key, float value) {
            int index = _otherMap.index(key);
            if (index >= 0 && eq(value, _otherMap.get(key))) {
                return true;
            }
            return false;
        }

        /**
         * Compare two floats for equality.
         */
        private final boolean eq(float v1, float v2) {
            return v1 == v2;
        }

    }

    /**
     * removes the mapping at <tt>index</tt> from the map.
     *
     * @param index an <code>int</code> value
     */
    protected void removeAt(int index) {
        _values[index] = (float) 0;
        super.removeAt(index);  // clear key, state; adjust size
    }

    /**
     * Returns the values of the map.
     *
     * @return a <code>Collection</code> value
     */
    public float[] getValues() {
        float[] vals = new float[size()];
        float[] v = _values;
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
    public int[] keys() {
        int[] keys = new int[size()];
        int[] k = _set;
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
    public int[] keys(int[] a) {
        int size = size();
        if (a.length < size) {
            a = (int[]) java.lang.reflect.Array.newInstance(
                    a.getClass().getComponentType(), size);
        }

        int[] k = (int[]) _set;
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
     * @param val an <code>float</code> value
     * @return a <code>boolean</code> value
     */
    public boolean containsValue(float val) {
        byte[] states = _states;
        float[] vals = _values;

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
     * @param key an <code>int</code> value
     * @return a <code>boolean</code> value
     */
    public boolean containsKey(int key) {
        return contains(key);
    }

    /**
     * Executes <tt>procedure</tt> for each key in the map.
     *
     * @param procedure a <code>TIntProcedure</code> value
     * @return false if the loop over the keys terminated because
     *         the procedure returned false for some key.
     */
    public boolean forEachKey(TIntProcedure procedure) {
        return forEach(procedure);
    }

    /**
     * Executes <tt>procedure</tt> for each value in the map.
     *
     * @param procedure a <code>TFloatProcedure</code> value
     * @return false if the loop over the values terminated because
     *         the procedure returned false for some value.
     */
    public boolean forEachValue(TFloatProcedure procedure) {
        byte[] states = _states;
        float[] values = _values;
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
     * @param procedure a <code>TOIntFloatProcedure</code> value
     * @return false if the loop over the entries terminated because
     *         the procedure returned false for some entry.
     */
    public boolean forEachEntry(TIntFloatProcedure procedure) {
        byte[] states = _states;
        int[] keys = _set;
        float[] values = _values;
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
    public boolean retainEntries(TIntFloatProcedure procedure) {
        boolean modified = false;
        byte[] states = _states;
        int[] keys = _set;
        float[] values = _values;


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
     * @param function a <code>TFloatFunction</code> value
     */
    public void transformValues(TFloatFunction function) {
        byte[] states = _states;
        float[] values = _values;
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
    public boolean increment(int key) {
        return adjustValue(key, (float) 1);
    }

    /**
     * Adjusts the primitive value mapped to key.
     *
     * @param key    the key of the value to increment
     * @param amount the amount to adjust the value by.
     * @return true if a mapping was found and modified.
     */
    public boolean adjustValue(int key, float amount) {
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
    public float adjustOrPutValue(final int key, final float adjust_amount, final float put_amount) {
        int index = insertionIndex(key);
        final boolean isNewMapping;
        final float newValue;
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
            int key = in.readInt();
            float val = in.readFloat();
            put(key, val);
        }
    }

    public String toString() {
        final StringBuilder buf = new StringBuilder("{");
        forEachEntry(new TIntFloatProcedure() {
            private boolean first = true;

            public boolean execute(int key, float value) {
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
} // TIntFloatHashMap
