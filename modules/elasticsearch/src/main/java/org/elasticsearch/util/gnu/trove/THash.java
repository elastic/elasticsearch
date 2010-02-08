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


/**
 * Base class for hashtables that use open addressing to resolve
 * collisions.
 * <p/>
 * Created: Wed Nov 28 21:11:16 2001
 *
 * @author Eric D. Friedman
 * @author Rob Eden (auto-compaction)
 * @version $Id: THash.java,v 1.14 2008/10/08 16:39:10 robeden Exp $
 */

abstract public class THash implements Cloneable, Externalizable {
    static final long serialVersionUID = -1792948471915530295L;

    /**
     * the load above which rehashing occurs.
     */
    protected static final float DEFAULT_LOAD_FACTOR = 0.5f;

    /**
     * the default initial capacity for the hash table.  This is one
     * less than a prime value because one is added to it when
     * searching for a prime capacity to account for the free slot
     * required by open addressing. Thus, the real default capacity is
     * 11.
     */
    protected static final int DEFAULT_INITIAL_CAPACITY = 10;


    /**
     * the current number of occupied slots in the hash.
     */
    protected transient int _size;

    /**
     * the current number of free slots in the hash.
     */
    protected transient int _free;

    /**
     * Determines how full the internal table can become before
     * rehashing is required. This must be a value in the range: 0.0 <
     * loadFactor < 1.0.  The default value is 0.5, which is about as
     * large as you can get in open addressing without hurting
     * performance.  Cf. Knuth, Volume 3., Chapter 6.
     */
    protected float _loadFactor;

    /**
     * The maximum number of elements allowed without allocating more
     * space.
     */
    protected int _maxSize;


    /**
     * The number of removes that should be performed before an auto-compaction occurs.
     */
    protected int _autoCompactRemovesRemaining;

    /**
     * The auto-compaction factor for the table.
     *
     * @see #setAutoCompactionFactor
     */
    protected float _autoCompactionFactor;

    /**
     * @see #tempDisableAutoCompaction
     */
    private transient boolean _autoCompactTemporaryDisable = false;


    /**
     * Creates a new <code>THash</code> instance with the default
     * capacity and load factor.
     */
    public THash() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Creates a new <code>THash</code> instance with a prime capacity
     * at or near the specified capacity and with the default load
     * factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public THash(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Creates a new <code>THash</code> instance with a prime capacity
     * at or near the minimum needed to hold <tt>initialCapacity</tt>
     * elements with load factor <tt>loadFactor</tt> without triggering
     * a rehash.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     */
    public THash(int initialCapacity, float loadFactor) {
        super();
        _loadFactor = loadFactor;

        // Through testing, the load factor (especially the default load factor) has been
        // found to be a pretty good starting auto-compaction factor.
        _autoCompactionFactor = loadFactor;

        setUp(HashFunctions.fastCeil(initialCapacity / loadFactor));
    }

    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException cnse) {
            return null; // it's supported
        }
    }

    /**
     * Tells whether this set is currently holding any elements.
     *
     * @return a <code>boolean</code> value
     */
    public boolean isEmpty() {
        return 0 == _size;
    }

    /**
     * Returns the number of distinct elements in this collection.
     *
     * @return an <code>int</code> value
     */
    public int size() {
        return _size;
    }

    /**
     * @return the current physical capacity of the hash table.
     */
    abstract protected int capacity();

    /**
     * Ensure that this hashtable has sufficient capacity to hold
     * <tt>desiredCapacity<tt> <b>additional</b> elements without
     * requiring a rehash.  This is a tuning method you can call
     * before doing a large insert.
     *
     * @param desiredCapacity an <code>int</code> value
     */
    public void ensureCapacity(int desiredCapacity) {
        if (desiredCapacity > (_maxSize - size())) {
            rehash(PrimeFinder.nextPrime(HashFunctions.fastCeil(
                    (desiredCapacity + size()) / _loadFactor) + 1));
            computeMaxSize(capacity());
        }
    }

    /**
     * Compresses the hashtable to the minimum prime size (as defined
     * by PrimeFinder) that will hold all of the elements currently in
     * the table.  If you have done a lot of <tt>remove</tt>
     * operations and plan to do a lot of queries or insertions or
     * iteration, it is a good idea to invoke this method.  Doing so
     * will accomplish two things:
     * <p/>
     * <ol>
     * <li> You'll free memory allocated to the table but no
     * longer needed because of the remove()s.</li>
     * <p/>
     * <li> You'll get better query/insert/iterator performance
     * because there won't be any <tt>REMOVED</tt> slots to skip
     * over when probing for indices in the table.</li>
     * </ol>
     */
    public void compact() {
        // need at least one free spot for open addressing
        rehash(PrimeFinder.nextPrime(HashFunctions.fastCeil(size() / _loadFactor) + 1));
        computeMaxSize(capacity());

        // If auto-compaction is enabled, re-determine the compaction interval
        if (_autoCompactionFactor != 0) {
            computeNextAutoCompactionAmount(size());
        }
    }


    /**
     * The auto-compaction factor controls whether and when a table performs a
     * {@link #compact} automatically after a certain number of remove operations.
     * If the value is non-zero, the number of removes that need to occur for
     * auto-compaction is the size of table at the time of the previous compaction
     * (or the initial capacity) multiplied by this factor.
     * <p/>
     * Setting this value to zero will disable auto-compaction.
     */
    public void setAutoCompactionFactor(float factor) {
        if (factor < 0) {
            throw new IllegalArgumentException("Factor must be >= 0: " + factor);
        }

        _autoCompactionFactor = factor;
    }

    /**
     * @see #setAutoCompactionFactor
     */
    public float getAutoCompactionFactor() {
        return _autoCompactionFactor;
    }


    /**
     * This simply calls {@link #compact compact}.  It is included for
     * symmetry with other collection classes.  Note that the name of this
     * method is somewhat misleading (which is why we prefer
     * <tt>compact</tt>) as the load factor may require capacity above
     * and beyond the size of this collection.
     *
     * @see #compact
     */
    public final void trimToSize() {
        compact();
    }

    /**
     * Delete the record at <tt>index</tt>.  Reduces the size of the
     * collection by one.
     *
     * @param index an <code>int</code> value
     */
    protected void removeAt(int index) {
        _size--;

        // If auto-compaction is enabled, see if we need to compact
        if (_autoCompactionFactor != 0) {
            _autoCompactRemovesRemaining--;

            if (!_autoCompactTemporaryDisable && _autoCompactRemovesRemaining <= 0) {
                // Do the compact
                // NOTE: this will cause the next compaction interval to be calculated
                compact();
            }
        }
    }

    /**
     * Empties the collection.
     */
    public void clear() {
        _size = 0;
        _free = capacity();
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

        capacity = PrimeFinder.nextPrime(initialCapacity);
        computeMaxSize(capacity);
        computeNextAutoCompactionAmount(initialCapacity);

        return capacity;
    }

    /**
     * Rehashes the set.
     *
     * @param newCapacity an <code>int</code> value
     */
    protected abstract void rehash(int newCapacity);

    /**
     * Temporarily disables auto-compaction. MUST be followed by calling
     * {@link #reenableAutoCompaction}.
     */
    protected void tempDisableAutoCompaction() {
        _autoCompactTemporaryDisable = true;
    }

    /**
     * Re-enable auto-compaction after it was disabled via
     * {@link #tempDisableAutoCompaction()}.
     *
     * @param check_for_compaction True if compaction should be performed if needed
     *                             before returning. If false, no compaction will be
     *                             performed.
     */
    protected void reenableAutoCompaction(boolean check_for_compaction) {
        _autoCompactTemporaryDisable = false;

        if (check_for_compaction && _autoCompactRemovesRemaining <= 0 &&
                _autoCompactionFactor != 0) {

            // Do the compact
            // NOTE: this will cause the next compaction interval to be calculated
            compact();
        }
    }


    /**
     * Computes the values of maxSize. There will always be at least
     * one free slot required.
     *
     * @param capacity an <code>int</code> value
     */
    private void computeMaxSize(int capacity) {
        // need at least one free slot for open addressing
        _maxSize = Math.min(capacity - 1, (int) (capacity * _loadFactor));
        _free = capacity - _size; // reset the free element count
    }


    /**
     * Computes the number of removes that need to happen before the next auto-compaction
     * will occur.
     */
    private void computeNextAutoCompactionAmount(int size) {
        if (_autoCompactionFactor != 0) {
            // NOTE: doing the round ourselves has been found to be faster than using
            //       Math.round.
            _autoCompactRemovesRemaining =
                    (int) ((size * _autoCompactionFactor) + 0.5f);
        }
    }


    /**
     * After an insert, this hook is called to adjust the size/free
     * values of the set and to perform rehashing if necessary.
     */
    protected final void postInsertHook(boolean usedFreeSlot) {
        if (usedFreeSlot) {
            _free--;
        }

        // rehash whenever we exhaust the available space in the table
        if (++_size > _maxSize || _free == 0) {
            // choose a new capacity suited to the new state of the table
            // if we've grown beyond our maximum size, double capacity;
            // if we've exhausted the free spots, rehash to the same capacity,
            // which will free up any stale removed slots for reuse.
            int newCapacity = _size > _maxSize ? PrimeFinder.nextPrime(capacity() << 1) : capacity();
            rehash(newCapacity);
            computeMaxSize(capacity());
        }
    }

    protected int calculateGrownCapacity() {
        return capacity() << 1;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        // VERSION
        out.writeByte(0);

        // LOAD FACTOR
        out.writeFloat(_loadFactor);

        // AUTO COMPACTION LOAD FACTOR
        out.writeFloat(_autoCompactionFactor);
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {

        // VERSION
        in.readByte();

        // LOAD FACTOR
        float old_factor = _loadFactor;
        _loadFactor = in.readFloat();

        // AUTO COMPACTION LOAD FACTOR
        _autoCompactionFactor = in.readFloat();


        // If we change the laod factor from the default, re-setup
        if (old_factor != _loadFactor) {
            setUp((int) Math.ceil(DEFAULT_INITIAL_CAPACITY / _loadFactor));
        }
    }
}// THash
