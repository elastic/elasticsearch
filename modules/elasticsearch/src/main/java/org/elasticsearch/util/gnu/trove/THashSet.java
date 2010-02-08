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
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * An implementation of the <tt>Set</tt> interface that uses an
 * open-addressed hash table to store its contents.
 * <p/>
 * Created: Sat Nov  3 10:38:17 2001
 *
 * @author Eric D. Friedman
 * @version $Id: THashSet.java,v 1.21 2008/10/07 20:33:56 robeden Exp $
 */

public class THashSet<E> extends TObjectHash<E>
        implements Set<E>, Iterable<E>, Externalizable {

    static final long serialVersionUID = 1L;

    /**
     * Creates a new <code>THashSet</code> instance with the default
     * capacity and load factor.
     */
    public THashSet() {
        super();
    }

    /**
     * Creates a new <code>THashSet</code> instance with the default
     * capacity and load factor.
     *
     * @param strategy used to compute hash codes and to compare objects.
     */
    public THashSet(TObjectHashingStrategy<E> strategy) {
        super(strategy);
    }

    /**
     * Creates a new <code>THashSet</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public THashSet(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Creates a new <code>THashSet</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param strategy        used to compute hash codes and to compare objects.
     */
    public THashSet(int initialCapacity, TObjectHashingStrategy<E> strategy) {
        super(initialCapacity, strategy);
    }

    /**
     * Creates a new <code>THashSet</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the specified load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     */
    public THashSet(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    /**
     * Creates a new <code>THashSet</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the specified load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     * @param strategy        used to compute hash codes and to compare objects.
     */
    public THashSet(int initialCapacity, float loadFactor, TObjectHashingStrategy<E> strategy) {
        super(initialCapacity, loadFactor, strategy);
    }

    /**
     * Creates a new <code>THashSet</code> instance containing the
     * elements of <tt>collection</tt>.
     *
     * @param collection a <code>Collection</code> value
     */
    public THashSet(Collection<? extends E> collection) {
        this(collection.size());
        addAll(collection);
    }

    /**
     * Creates a new <code>THashSet</code> instance containing the
     * elements of <tt>collection</tt>.
     *
     * @param collection a <code>Collection</code> value
     * @param strategy   used to compute hash codes and to compare objects.
     */
    public THashSet(Collection<? extends E> collection, TObjectHashingStrategy<E> strategy) {
        this(collection.size(), strategy);
        addAll(collection);
    }

    /**
     * Inserts a value into the set.
     *
     * @param obj an <code>Object</code> value
     * @return true if the set was modified by the add operation
     */
    public boolean add(E obj) {
        int index = insertionIndex(obj);

        if (index < 0) {
            return false;       // already present in set, nothing to add
        }

        Object old = _set[index];
        _set[index] = obj;

        postInsertHook(old == FREE);
        return true;            // yes, we added something
    }

    public boolean equals(Object other) {
        if (!(other instanceof Set)) {
            return false;
        }
        Set that = (Set) other;
        if (that.size() != this.size()) {
            return false;
        }
        return containsAll(that);
    }

    public int hashCode() {
        HashProcedure p = new HashProcedure();
        forEach(p);
        return p.getHashCode();
    }

    private final class HashProcedure implements TObjectProcedure<E> {
        private int h = 0;

        public int getHashCode() {
            return h;
        }

        public final boolean execute(E key) {
            h += _hashingStrategy.computeHashCode(key);
            return true;
        }
    }

    /**
     * Expands the set to accommodate new values.
     *
     * @param newCapacity an <code>int</code> value
     */
    protected void rehash(int newCapacity) {
        int oldCapacity = _set.length;
        Object oldSet[] = _set;

        _set = new Object[newCapacity];
        Arrays.fill(_set, FREE);

        for (int i = oldCapacity; i-- > 0;) {
            if (oldSet[i] != FREE && oldSet[i] != REMOVED) {
                E o = (E) oldSet[i];
                int index = insertionIndex(o);
                if (index < 0) { // everyone pays for this because some people can't RTFM
                    throwObjectContractViolation(_set[(-index - 1)], o);
                }
                _set[index] = o;
            }
        }
    }

    /**
     * Returns a new array containing the objects in the set.
     *
     * @return an <code>Object[]</code> value
     */
    public Object[] toArray() {
        Object[] result = new Object[size()];
        forEach(new ToObjectArrayProcedure(result));
        return result;
    }

    /**
     * Returns a typed array of the objects in the set.
     *
     * @param a an <code>Object[]</code> value
     * @return an <code>Object[]</code> value
     */
    public <T> T[] toArray(T[] a) {
        int size = size();
        if (a.length < size)
            a = (T[]) Array.newInstance(a.getClass().getComponentType(), size);

        forEach(new ToObjectArrayProcedure(a));

        // If this collection fits in the specified array with room to
        // spare (i.e., the array has more elements than this
        // collection), the element in the array immediately following
        // the end of the collection is set to null. This is useful in
        // determining the length of this collection only if the
        // caller knows that this collection does not contain any null
        // elements.)

        if (a.length > size) {
            a[size] = null;
        }

        return a;
    }

    /**
     * Empties the set.
     */
    public void clear() {
        super.clear();

        Arrays.fill(_set, 0, _set.length, FREE);
    }

    /**
     * Removes <tt>obj</tt> from the set.
     *
     * @param obj an <code>Object</code> value
     * @return true if the set was modified by the remove operation.
     */
    public boolean remove(Object obj) {
        int index = index((E) obj);
        if (index >= 0) {
            removeAt(index);
            return true;
        }
        return false;
    }

    /**
     * Creates an iterator over the values of the set.  The iterator
     * supports element deletion.
     *
     * @return an <code>Iterator</code> value
     */
    public Iterator<E> iterator() {
        return new TObjectHashIterator<E>(this);
    }

    /**
     * Tests the set to determine if all of the elements in
     * <tt>collection</tt> are present.
     *
     * @param collection a <code>Collection</code> value
     * @return true if all elements were present in the set.
     */
    public boolean containsAll(Collection<?> collection) {
        for (Iterator i = collection.iterator(); i.hasNext();) {
            if (!contains(i.next())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Adds all of the elements in <tt>collection</tt> to the set.
     *
     * @param collection a <code>Collection</code> value
     * @return true if the set was modified by the add all operation.
     */
    public boolean addAll(Collection<? extends E> collection) {
        boolean changed = false;
        int size = collection.size();

        ensureCapacity(size);
        Iterator<? extends E> it = collection.iterator();
        while (size-- > 0) {
            if (add(it.next())) {
                changed = true;
            }
        }
        return changed;
    }

    /**
     * Removes all of the elements in <tt>collection</tt> from the set.
     *
     * @param collection a <code>Collection</code> value
     * @return true if the set was modified by the remove all operation.
     */
    public boolean removeAll(Collection<?> collection) {
        boolean changed = false;
        int size = collection.size();
        Iterator it;

        it = collection.iterator();
        while (size-- > 0) {
            if (remove(it.next())) {
                changed = true;
            }
        }
        return changed;
    }

    /**
     * Removes any values in the set which are not contained in
     * <tt>collection</tt>.
     *
     * @param collection a <code>Collection</code> value
     * @return true if the set was modified by the retain all operation
     */
    public boolean retainAll(Collection<?> collection) {
        boolean changed = false;
        int size = size();
        Iterator it;

        it = iterator();
        while (size-- > 0) {
            if (!collection.contains(it.next())) {
                it.remove();
                changed = true;
            }
        }
        return changed;
    }

    public String toString() {
        final StringBuilder buf = new StringBuilder("{");
        forEach(new TObjectProcedure() {
            private boolean first = true;

            public boolean execute(Object value) {
                if (first) first = false;
                else buf.append(",");

                buf.append(value);
                return true;
            }
        });
        buf.append("}");
        return buf.toString();
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
        if (!forEach(writeProcedure)) {
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
            E val = (E) in.readObject();
            add(val);
        }
    }
} // THashSet
