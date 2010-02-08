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
import java.util.*;

/**
 * An implementation of the Map interface which uses an open addressed
 * hash table to store its contents.
 * <p/>
 * Created: Sun Nov  4 08:52:45 2001
 *
 * @author Eric D. Friedman
 * @version $Id: THashMap.java,v 1.33 2008/05/08 17:42:55 robeden Exp $
 */
public class THashMap<K, V> extends TObjectHash<K> implements Map<K, V>, Externalizable {
    static final long serialVersionUID = 1L;

    /**
     * the values of the  map
     */
    protected transient V[] _values;

    /**
     * Creates a new <code>THashMap</code> instance with the default
     * capacity and load factor.
     */
    public THashMap() {
        super();
    }

    /**
     * Creates a new <code>THashMap</code> instance with the default
     * capacity and load factor.
     *
     * @param strategy used to compute hash codes and to compare objects.
     */
    public THashMap(TObjectHashingStrategy<K> strategy) {
        super(strategy);
    }

    /**
     * Creates a new <code>THashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public THashMap(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Creates a new <code>THashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param strategy        used to compute hash codes and to compare objects.
     */
    public THashMap(int initialCapacity, TObjectHashingStrategy<K> strategy) {
        super(initialCapacity, strategy);
    }

    /**
     * Creates a new <code>THashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the specified load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     */
    public THashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    /**
     * Creates a new <code>THashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the specified load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     * @param strategy        used to compute hash codes and to compare objects.
     */
    public THashMap(int initialCapacity, float loadFactor, TObjectHashingStrategy<K> strategy) {
        super(initialCapacity, loadFactor, strategy);
    }

    /**
     * Creates a new <code>THashMap</code> instance which contains the
     * key/value pairs in <tt>map</tt>.
     *
     * @param map a <code>Map</code> value
     */
    public THashMap(Map<K, V> map) {
        this(map.size());
        putAll(map);
    }

    /**
     * Creates a new <code>THashMap</code> instance which contains the
     * key/value pairs in <tt>map</tt>.
     *
     * @param map      a <code>Map</code> value
     * @param strategy used to compute hash codes and to compare objects.
     */
    public THashMap(Map<K, V> map, TObjectHashingStrategy<K> strategy) {
        this(map.size(), strategy);
        putAll(map);
    }

    /**
     * @return a shallow clone of this collection
     */
    public THashMap<K, V> clone() {
        THashMap<K, V> m = (THashMap<K, V>) super.clone();
        m._values = this._values.clone();
        return m;
    }

    /**
     * initialize the value array of the map.
     *
     * @param initialCapacity an <code>int</code> value
     * @return an <code>int</code> value
     */
    protected int setUp(int initialCapacity) {
        int capacity;

        capacity = super.setUp(initialCapacity);
        //noinspection unchecked
        _values = (V[]) new Object[capacity];
        return capacity;
    }

    /**
     * Inserts a key/value pair into the map.
     *
     * @param key   an <code>Object</code> value
     * @param value an <code>Object</code> value
     * @return the previous value associated with <tt>key</tt>,
     *         or {@code null} if none was found.
     */
    public V put(K key, V value) {
        int index = insertionIndex(key);
        return doPut(key, value, index);
    }

    /**
     * Inserts a key/value pair into the map if the specified key is not already
     * associated with a value.
     *
     * @param key   an <code>Object</code> value
     * @param value an <code>Object</code> value
     * @return the previous value associated with <tt>key</tt>,
     *         or {@code null} if none was found.
     */
    public V putIfAbsent(K key, V value) {
        int index = insertionIndex(key);
        if (index < 0)
            return _values[-index - 1];
        return doPut(key, value, index);
    }

    private V doPut(K key, V value, int index) {
        V previous = null;
        Object oldKey;
        boolean isNewMapping = true;
        if (index < 0) {
            index = -index - 1;
            previous = _values[index];
            isNewMapping = false;
        }
        oldKey = _set[index];
        _set[index] = key;
        _values[index] = value;
        if (isNewMapping) {
            postInsertHook(oldKey == FREE);
        }

        return previous;
    }

    /**
     * Compares this map with another map for equality of their stored
     * entries.
     *
     * @param other an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    public boolean equals(Object other) {
        if (!(other instanceof Map)) {
            return false;
        }
        Map<K, V> that = (Map<K, V>) other;
        if (that.size() != this.size()) {
            return false;
        }
        return forEachEntry(new EqProcedure<K, V>(that));
    }

    public int hashCode() {
        HashProcedure p = new HashProcedure();
        forEachEntry(p);
        return p.getHashCode();
    }

    public String toString() {
        final StringBuilder buf = new StringBuilder("{");
        forEachEntry(new TObjectObjectProcedure<K, V>() {
            private boolean first = true;

            public boolean execute(K key, V value) {
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

    private final class HashProcedure implements TObjectObjectProcedure<K, V> {
        private int h = 0;

        public int getHashCode() {
            return h;
        }

        public final boolean execute(K key, V value) {
            h += _hashingStrategy.computeHashCode(key) ^ (value == null ? 0 : value.hashCode());
            return true;
        }
    }

    private static final class EqProcedure<K, V> implements TObjectObjectProcedure<K, V> {
        private final Map<K, V> _otherMap;

        EqProcedure(Map<K, V> otherMap) {
            _otherMap = otherMap;
        }

        public final boolean execute(K key, V value) {
            // Check to make sure the key is there. This avoids problems that come up with
            // null values. Since it is only caused in that cause, only do this when the
            // value is null (to avoid extra work).
            if (value == null && !_otherMap.containsKey(key)) return false;

            V oValue = _otherMap.get(key);
            return oValue == value || (oValue != null && oValue.equals(value));
        }
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
     * @param procedure a <code>TObjectProcedure</code> value
     * @return false if the loop over the values terminated because
     *         the procedure returned false for some value.
     */
    public boolean forEachValue(TObjectProcedure<V> procedure) {
        V[] values = _values;
        Object[] set = _set;
        for (int i = values.length; i-- > 0;) {
            if (set[i] != FREE
                    && set[i] != REMOVED
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
     * @param procedure a <code>TObjectObjectProcedure</code> value
     * @return false if the loop over the entries terminated because
     *         the procedure returned false for some entry.
     */
    public boolean forEachEntry(TObjectObjectProcedure<K, V> procedure) {
        Object[] keys = _set;
        V[] values = _values;
        for (int i = keys.length; i-- > 0;) {
            if (keys[i] != FREE
                    && keys[i] != REMOVED
                    && !procedure.execute((K) keys[i], values[i])) {
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
    public boolean retainEntries(TObjectObjectProcedure<K, V> procedure) {
        boolean modified = false;
        Object[] keys = _set;
        V[] values = _values;

        // Temporarily disable compaction. This is a fix for bug #1738760
        tempDisableAutoCompaction();
        try {
            for (int i = keys.length; i-- > 0;) {
                if (keys[i] != FREE
                        && keys[i] != REMOVED
                        && !procedure.execute((K) keys[i], values[i])) {
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
     * @param function a <code>TObjectFunction</code> value
     */
    public void transformValues(TObjectFunction<V, V> function) {
        V[] values = _values;
        Object[] set = _set;
        for (int i = values.length; i-- > 0;) {
            if (set[i] != FREE && set[i] != REMOVED) {
                values[i] = function.execute(values[i]);
            }
        }
    }

    /**
     * rehashes the map to the new capacity.
     *
     * @param newCapacity an <code>int</code> value
     */
    protected void rehash(int newCapacity) {
        int oldCapacity = _set.length;
        Object oldKeys[] = _set;
        V oldVals[] = _values;

        _set = new Object[newCapacity];
        Arrays.fill(_set, FREE);
        _values = (V[]) new Object[newCapacity];

        for (int i = oldCapacity; i-- > 0;) {
            if (oldKeys[i] != FREE && oldKeys[i] != REMOVED) {
                Object o = oldKeys[i];
                int index = insertionIndex((K) o);
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
     * @return the value of <tt>key</tt> or null if no such mapping exists.
     */
    public V get(Object key) {
        int index = index((K) key);
        return index < 0 ? null : _values[index];
    }

    /**
     * Empties the map.
     */
    public void clear() {
        if (size() == 0) return; // optimization

        super.clear();

        Arrays.fill(_set, 0, _set.length, FREE);
        Arrays.fill(_values, 0, _values.length, null);
    }

    /**
     * Deletes a key/value pair from the map.
     *
     * @param key an <code>Object</code> value
     * @return an <code>Object</code> value
     */
    public V remove(Object key) {
        V prev = null;
        int index = index((K) key);
        if (index >= 0) {
            prev = _values[index];
            removeAt(index);    // clear key,state; adjust size
        }
        return prev;
    }

    /**
     * removes the mapping at <tt>index</tt> from the map.
     *
     * @param index an <code>int</code> value
     */
    protected void removeAt(int index) {
        _values[index] = null;
        super.removeAt(index);  // clear key, state; adjust size
    }

    /**
     * Returns a view on the values of the map.
     *
     * @return a <code>Collection</code> value
     */
    public Collection<V> values() {
        return new ValueView();
    }

    /**
     * returns a Set view on the keys of the map.
     *
     * @return a <code>Set</code> value
     */
    public Set<K> keySet() {
        return new KeyView();
    }

    /**
     * Returns a Set view on the entries of the map.
     *
     * @return a <code>Set</code> value
     */
    public Set<Map.Entry<K, V>> entrySet() {
        return new EntryView();
    }

    /**
     * checks for the presence of <tt>val</tt> in the values of the map.
     *
     * @param val an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    public boolean containsValue(Object val) {
        Object[] set = _set;
        V[] vals = _values;

        // special case null values so that we don't have to
        // perform null checks before every call to equals()
        if (null == val) {
            for (int i = vals.length; i-- > 0;) {
                if ((set[i] != FREE && set[i] != REMOVED) &&
                        val == vals[i]) {
                    return true;
                }
            }
        } else {
            for (int i = vals.length; i-- > 0;) {
                if ((set[i] != FREE && set[i] != REMOVED) &&
                        (val == vals[i] || val.equals(vals[i]))) {
                    return true;
                }
            }
        } // end of else
        return false;
    }

    /**
     * checks for the present of <tt>key</tt> in the keys of the map.
     *
     * @param key an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    public boolean containsKey(Object key) {
        return contains(key);
    }

    /**
     * copies the key/value mappings in <tt>map</tt> into this map.
     *
     * @param map a <code>Map</code> value
     */
    public void putAll(Map<? extends K, ? extends V> map) {
        ensureCapacity(map.size());
        // could optimize this for cases when map instanceof THashMap
        for (Iterator<? extends Map.Entry<? extends K, ? extends V>> i = map.entrySet().iterator(); i.hasNext();) {
            Map.Entry<? extends K, ? extends V> e = i.next();
            put(e.getKey(), e.getValue());
        }
    }

    /**
     * a view onto the values of the map.
     */
    protected class ValueView extends MapBackedView<V> {
        public Iterator<V> iterator() {
            return new THashIterator<V>(THashMap.this) {
                protected V objectAtIndex(int index) {
                    return _values[index];
                }
            };
        }

        public boolean containsElement(V value) {
            return containsValue(value);
        }

        public boolean removeElement(V value) {
            Object[] values = _values;
            Object[] set = _set;

            for (int i = values.length; i-- > 0;) {
                if ((set[i] != FREE && set[i] != REMOVED) &&
                        value == values[i] ||
                        (null != values[i] && values[i].equals(value))) {

                    removeAt(i);
                    return true;
                }
            }

            return false;
        }
    }

    /**
     * a view onto the entries of the map.
     */
    protected class EntryView extends MapBackedView<Map.Entry<K, V>> {
        private final class EntryIterator extends THashIterator<Map.Entry<K, V>> {
            EntryIterator(THashMap<K, V> map) {
                super(map);
            }

            public Entry objectAtIndex(final int index) {
                return new Entry((K) _set[index], _values[index], index);
            }
        }

        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntryIterator(THashMap.this);
        }

        public boolean removeElement(Map.Entry<K, V> entry) {
            // have to effectively reimplement Map.remove here
            // because we need to return true/false depending on
            // whether the removal took place.  Since the Entry's
            // value can be null, this means that we can't rely
            // on the value of the object returned by Map.remove()
            // to determine whether a deletion actually happened.
            //
            // Note also that the deletion is only legal if
            // both the key and the value match.
            Object val;
            int index;

            K key = keyForEntry(entry);
            index = index(key);
            if (index >= 0) {
                val = valueForEntry(entry);
                if (val == _values[index] ||
                        (null != val && val.equals(_values[index]))) {
                    removeAt(index);    // clear key,state; adjust size
                    return true;
                }
            }
            return false;
        }

        public boolean containsElement(Map.Entry<K, V> entry) {
            Object val = get(keyForEntry(entry));
            Object entryValue = entry.getValue();
            return entryValue == val ||
                    (null != val && val.equals(entryValue));
        }

        protected V valueForEntry(Map.Entry<K, V> entry) {
            return entry.getValue();
        }

        protected K keyForEntry(Map.Entry<K, V> entry) {
            return entry.getKey();
        }
    }

    private abstract class MapBackedView<E> extends AbstractSet<E>
            implements Set<E>, Iterable<E> {

        public abstract Iterator<E> iterator();

        public abstract boolean removeElement(E key);

        public abstract boolean containsElement(E key);

        public boolean contains(Object key) {
            return containsElement((E) key);
        }

        public boolean remove(Object o) {
            return removeElement((E) o);
        }

        public boolean containsAll(Collection<?> collection) {
            for (Iterator i = collection.iterator(); i.hasNext();) {
                if (!contains(i.next())) {
                    return false;
                }
            }
            return true;
        }

        public void clear() {
            THashMap.this.clear();
        }

        public boolean add(E obj) {
            throw new UnsupportedOperationException();
        }

        public int size() {
            return THashMap.this.size();
        }

        public Object[] toArray() {
            Object[] result = new Object[size()];
            Iterator e = iterator();
            for (int i = 0; e.hasNext(); i++)
                result[i] = e.next();
            return result;
        }

        public <T> T[] toArray(T[] a) {
            int size = size();
            if (a.length < size)
                a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);

            Iterator<E> it = iterator();
            Object[] result = a;
            for (int i = 0; i < size; i++) {
                result[i] = it.next();
            }

            if (a.length > size) {
                a[size] = null;
            }

            return a;
        }

        public boolean isEmpty() {
            return THashMap.this.isEmpty();
        }

        public boolean addAll(Collection<? extends E> collection) {
            throw new UnsupportedOperationException();
        }

        public boolean retainAll(Collection<?> collection) {
            boolean changed = false;
            Iterator i = iterator();
            while (i.hasNext()) {
                if (!collection.contains(i.next())) {
                    i.remove();
                    changed = true;
                }
            }
            return changed;
        }
    }

    /**
     * a view onto the keys of the map.
     */
    protected class KeyView extends MapBackedView<K> {
        public Iterator<K> iterator() {
            return new TObjectHashIterator<K>(THashMap.this);
        }

        public boolean removeElement(K key) {
            return null != THashMap.this.remove(key);
        }

        public boolean containsElement(K key) {
            return THashMap.this.contains(key);
        }
    }

    final class Entry implements Map.Entry<K, V> {
        private K key;
        private V val;
        private final int index;

        Entry(final K key, V value, final int index) {
            this.key = key;
            this.val = value;
            this.index = index;
        }

        void setKey(K aKey) {
            this.key = aKey;
        }

        void setValue0(V aValue) {
            this.val = aValue;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return val;
        }

        public V setValue(V o) {
            if (_values[index] != val) {
                throw new ConcurrentModificationException();
            }
            _values[index] = o;
            o = val;            // need to return previous value
            val = o;            // update this entry's value, in case
            // setValue is called again
            return o;
        }

        public boolean equals(Object o) {
            if (o instanceof Map.Entry) {
                Map.Entry e1 = this;
                Map.Entry e2 = (Map.Entry) o;
                return (e1.getKey() == null ? e2.getKey() == null : e1.getKey().equals(e2.getKey()))
                        && (e1.getValue() == null ? e2.getValue() == null : e1.getValue().equals(e2.getValue()));
            }
            return false;
        }

        public int hashCode() {
            return (getKey() == null ? 0 : getKey().hashCode()) ^ (getValue() == null ? 0 : getValue().hashCode());
        }
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        // VERSION
        out.writeByte(1);

        // NOTE: Super was not written in version 0
        super.writeExternal(out);

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
        byte version = in.readByte();

        // NOTE: super was not written in version 0
        if (version != 0) super.readExternal(in);

        // NUMBER OF ENTRIES
        int size = in.readInt();
        setUp(size);

        // ENTRIES
        while (size-- > 0) {
            //noinspection unchecked
            K key = (K) in.readObject();
            //noinspection unchecked
            V val = (V) in.readObject();
            put(key, val);
        }
    }
} // THashMap
