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

/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.elasticsearch.common.util.concurrent.highscalelib;

import sun.misc.Unsafe;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A multi-threaded bit-vector set, implemented as an array of primitive
 * {@code longs}.  All operations are non-blocking and multi-threaded safe.
 * {@link #contains(int)} calls are roughly the same speed as a {load, mask}
 * sequence.  {@link #add(int)} and {@link #remove(int)} calls are a tad more
 * expensive than a {load, mask, store} sequence because they must use a CAS.
 * The bit-vector is auto-sizing.
 * <p/>
 * <p><em>General note of caution:</em> The Set API allows the use of {@link Integer}
 * with silent autoboxing - which can be very expensive if many calls are
 * being made.  Since autoboxing is silent you may not be aware that this is
 * going on.  The built-in API takes lower-case {@code ints} and is much more
 * efficient.
 * <p/>
 * <p>Space: space is used in proportion to the largest element, as opposed to
 * the number of elements (as is the case with hash-table based Set
 * implementations).  Space is approximately (largest_element/8 + 64) bytes.
 * <p/>
 * The implementation is a simple bit-vector using CAS for update.
 *
 * @author Cliff Click
 * @since 1.5
 */

public class NonBlockingSetInt extends AbstractSet<Integer> implements Serializable {
    private static final long serialVersionUID = 1234123412341234123L;
    private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();

    // --- Bits to allow atomic update of the NBSI
    private static final long _nbsi_offset;

    static {                      // <clinit>
        Field f = null;
        try {
            f = NonBlockingSetInt.class.getDeclaredField("_nbsi");
        } catch (java.lang.NoSuchFieldException e) {
        }
        _nbsi_offset = _unsafe.objectFieldOffset(f);
    }

    private final boolean CAS_nbsi(NBSI old, NBSI nnn) {
        return _unsafe.compareAndSwapObject(this, _nbsi_offset, old, nnn);
    }

    // The actual Set of Joy, which changes during a resize event.  The
    // Only Field for this class, so I can atomically change the entire
    // set implementation with a single CAS.
    private transient NBSI _nbsi;

    /**
     * Create a new empty bit-vector
     */
    public NonBlockingSetInt() {
        _nbsi = new NBSI(63, new Counter(), this); // The initial 1-word set
    }

    /**
     * Add {@code i} to the set.  Uppercase {@link Integer} version of add,
     * requires auto-unboxing.  When possible use the {@code int} version of
     * {@link #add(int)} for efficiency.
     *
     * @return <tt>true</tt> if i was added to the set.
     * @throws IllegalArgumentException if i is negative.
     */
    public boolean add(final Integer i) {
        return add(i.intValue());
    }

    /**
     * Test if {@code o} is in the set.  This is the uppercase {@link Integer}
     * version of contains, requires a type-check and auto-unboxing.  When
     * possible use the {@code int} version of {@link #contains(int)} for
     * efficiency.
     *
     * @return <tt>true</tt> if i was in the set.
     */
    public boolean contains(final Object o) {
        return o instanceof Integer ? contains(((Integer) o).intValue()) : false;
    }

    /**
     * Remove {@code o} from the set.  This is the uppercase {@link Integer}
     * version of remove, requires a type-check and auto-unboxing.  When
     * possible use the {@code int} version of {@link #remove(int)} for
     * efficiency.
     *
     * @return <tt>true</tt> if i was removed to the set.
     */
    public boolean remove(final Object o) {
        return o instanceof Integer ? remove(((Integer) o).intValue()) : false;
    }

    /**
     * Add {@code i} to the set.  This is the lower-case '{@code int}' version
     * of {@link #add} - no autoboxing.  Negative values throw
     * IllegalArgumentException.
     *
     * @return <tt>true</tt> if i was added to the set.
     * @throws IllegalArgumentException if i is negative.
     */
    public boolean add(final int i) {
        if (i < 0) throw new IllegalArgumentException("" + i);
        return _nbsi.add(i);
    }

    /**
     * Test if {@code i} is in the set.  This is the lower-case '{@code int}'
     * version of {@link #contains} - no autoboxing.
     *
     * @return <tt>true</tt> if i was int the set.
     */
    public boolean contains(final int i) {
        return i < 0 ? false : _nbsi.contains(i);
    }

    /**
     * Remove {@code i} from the set.  This is the fast lower-case '{@code int}'
     * version of {@link #remove} - no autoboxing.
     *
     * @return <tt>true</tt> if i was added to the set.
     */
    public boolean remove(final int i) {
        return i < 0 ? false : _nbsi.remove(i);
    }

    /**
     * Current count of elements in the set.  Due to concurrent racing updates,
     * the size is only ever approximate.  Updates due to the calling thread are
     * immediately visible to calling thread.
     *
     * @return count of elements.
     */
    public int size() {
        return _nbsi.size();
    }

    /**
     * Empty the bitvector.
     */
    public void clear() {
        NBSI cleared = new NBSI(63, new Counter(), this); // An empty initial NBSI
        while (!CAS_nbsi(_nbsi, cleared)) // Spin until clear works
            ;
    }

    /**
     * Verbose printout of internal structure for debugging.
     */
    public void print() {
        _nbsi.print(0);
    }

    /**
     * Standard Java {@link Iterator}.  Not very efficient because it
     * auto-boxes the returned values.
     */
    public Iterator<Integer> iterator() {
        return new iter();
    }

    private class iter implements Iterator<Integer> {
        NBSI _nbsi2;
        int _idx = -1;
        int _prev = -1;

        iter() {
            _nbsi2 = _nbsi;
            advance();
        }

        public boolean hasNext() {
            return _idx != -2;
        }

        private void advance() {
            while (true) {
                _idx++;                 // Next index
                while ((_idx >> 6) >= _nbsi2._bits.length) { // Index out of range?
                    if (_nbsi2._new == null) { // New table?
                        _idx = -2;          // No, so must be all done
                        return;             //
                    }
                    _nbsi2 = _nbsi2._new; // Carry on, in the new table
                }
                if (_nbsi2.contains(_idx)) return;
            }
        }

        public Integer next() {
            if (_idx == -1) throw new NoSuchElementException();
            _prev = _idx;
            advance();
            return _prev;
        }

        public void remove() {
            if (_prev == -1) throw new IllegalStateException();
            _nbsi2.remove(_prev);
            _prev = -1;
        }
    }

    // --- writeObject -------------------------------------------------------
    // Write a NBSI to a stream

    private void writeObject(java.io.ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();     // Nothing to write
        final NBSI nbsi = _nbsi;    // The One Field is transient
        final int len = _nbsi._bits.length << 6;
        s.writeInt(len);            // Write max element
        for (int i = 0; i < len; i++)
            s.writeBoolean(_nbsi.contains(i));
    }

    // --- readObject --------------------------------------------------------
    // Read a CHM from a stream

    private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
        s.defaultReadObject();      // Read nothing
        final int len = s.readInt(); // Read max element
        _nbsi = new NBSI(len, new Counter(), this);
        for (int i = 0; i < len; i++)  // Read all bits
            if (s.readBoolean())
                _nbsi.add(i);
    }

    // --- NBSI ----------------------------------------------------------------

    private static final class NBSI {
        // Back pointer to the parent wrapper; sorta like make the class non-static
        private transient final NonBlockingSetInt _non_blocking_set_int;

        // Used to count elements: a high-performance counter.
        private transient final Counter _size;

        // The Bits
        private final long _bits[];
        // --- Bits to allow Unsafe access to arrays
        private static final int _Lbase = _unsafe.arrayBaseOffset(long[].class);
        private static final int _Lscale = _unsafe.arrayIndexScale(long[].class);

        private static long rawIndex(final long[] ary, final int idx) {
            assert idx >= 0 && idx < ary.length;
            return _Lbase + idx * _Lscale;
        }

        private final boolean CAS(int idx, long old, long nnn) {
            return _unsafe.compareAndSwapLong(_bits, rawIndex(_bits, idx), old, nnn);
        }

        // --- Resize
        // The New Table, only set once to non-zero during a resize.
        // Must be atomically set.
        private NBSI _new;
        private static final long _new_offset;

        static {                      // <clinit>
            Field f = null;
            try {
                f = NBSI.class.getDeclaredField("_new");
            } catch (java.lang.NoSuchFieldException e) {
            }
            _new_offset = _unsafe.objectFieldOffset(f);
        }

        private final boolean CAS_new(NBSI nnn) {
            return _unsafe.compareAndSwapObject(this, _new_offset, null, nnn);
        }

        private transient final AtomicInteger _copyIdx;   // Used to count bits started copying
        private transient final AtomicInteger _copyDone;  // Used to count words copied in a resize operation
        private transient final int _sum_bits_length; // Sum of all nested _bits.lengths

        private static final long mask(int i) {
            return 1L << (i & 63);
        }

        // I need 1 free bit out of 64 to allow for resize.  I do this by stealing
        // the high order bit - but then I need to do something with adding element
        // number 63 (and friends).  I could use a mod63 function but it's more
        // efficient to handle the mod-64 case as an exception.
        //
        // Every 64th bit is put in it's own recursive bitvector.  If the low 6 bits
        // are all set, we shift them off and recursively operate on the _nbsi64 set.
        private final NBSI _nbsi64;

        private NBSI(int max_elem, Counter ctr, NonBlockingSetInt nonb) {
            super();
            _non_blocking_set_int = nonb;
            _size = ctr;
            _copyIdx = ctr == null ? null : new AtomicInteger();
            _copyDone = ctr == null ? null : new AtomicInteger();
            // The main array of bits
            _bits = new long[(int) (((long) max_elem + 63) >>> 6)];
            // Every 64th bit is moved off to it's own subarray, so that the
            // sign-bit is free for other purposes
            _nbsi64 = ((max_elem + 1) >>> 6) == 0 ? null : new NBSI((max_elem + 1) >>> 6, null, null);
            _sum_bits_length = _bits.length + (_nbsi64 == null ? 0 : _nbsi64._sum_bits_length);
        }

        // Lower-case 'int' versions - no autoboxing, very fast.
        // 'i' is known positive.

        public boolean add(final int i) {
            // Check for out-of-range for the current size bit vector.
            // If so we need to grow the bit vector.
            if ((i >> 6) >= _bits.length)
                return install_larger_new_bits(i). // Install larger pile-o-bits (duh)
                        help_copy().add(i);              // Finally, add to the new table

            // Handle every 64th bit via using a nested array
            NBSI nbsi = this;         // The bit array being added into
            int j = i;                // The bit index being added
            while ((j & 63) == 63) {   // Bit 64? (low 6 bits are all set)
                nbsi = nbsi._nbsi64;    // Recurse
                j = j >> 6;               // Strip off low 6 bits (all set)
            }

            final long mask = mask(j);
            long old;
            do {
                old = nbsi._bits[j >> 6]; // Read old bits
                if (old < 0)           // Not mutable?
                    // Not mutable: finish copy of word, and retry on copied word
                    return help_copy_impl(i).help_copy().add(i);
                if ((old & mask) != 0) return false; // Bit is already set?
            } while (!nbsi.CAS(j >> 6, old, old | mask));
            _size.add(1);
            return true;
        }

        public boolean remove(final int i) {
            if ((i >> 6) >= _bits.length) // Out of bounds?  Not in this array!
                return _new == null ? false : help_copy().remove(i);

            // Handle every 64th bit via using a nested array
            NBSI nbsi = this;         // The bit array being added into
            int j = i;                // The bit index being added
            while ((j & 63) == 63) {   // Bit 64? (low 6 bits are all set)
                nbsi = nbsi._nbsi64;    // Recurse
                j = j >> 6;               // Strip off low 6 bits (all set)
            }

            final long mask = mask(j);
            long old;
            do {
                old = nbsi._bits[j >> 6]; // Read old bits
                if (old < 0)           // Not mutable?
                    // Not mutable: finish copy of word, and retry on copied word
                    return help_copy_impl(i).help_copy().remove(i);
                if ((old & mask) == 0) return false; // Bit is already clear?
            } while (!nbsi.CAS(j >> 6, old, old & ~mask));
            _size.add(-1);
            return true;
        }

        public boolean contains(final int i) {
            if ((i >> 6) >= _bits.length) // Out of bounds?  Not in this array!
                return _new == null ? false : help_copy().contains(i);

            // Handle every 64th bit via using a nested array
            NBSI nbsi = this;         // The bit array being added into
            int j = i;                // The bit index being added
            while ((j & 63) == 63) {   // Bit 64? (low 6 bits are all set)
                nbsi = nbsi._nbsi64;    // Recurse
                j = j >> 6;               // Strip off low 6 bits (all set)
            }

            final long mask = mask(j);
            long old = nbsi._bits[j >> 6]; // Read old bits
            if (old < 0)             // Not mutable?
                // Not mutable: finish copy of word, and retry on copied word
                return help_copy_impl(i).help_copy().contains(i);
            // Yes mutable: test & return bit
            return (old & mask) != 0;
        }

        public int size() {
            return (int) _size.get();
        }

        // Must grow the current array to hold an element of size i

        private NBSI install_larger_new_bits(final int i) {
            if (_new == null) {
                // Grow by powers of 2, to avoid minor grow-by-1's.
                // Note: must grow by exact powers-of-2 or the by-64-bit trick doesn't work right
                int sz = (_bits.length << 6) << 1;
                // CAS to install a new larger size.  Did it work?  Did it fail?  We
                // don't know and don't care.  Only One can be installed, so if
                // another thread installed a too-small size, we can't help it - we
                // must simply install our new larger size as a nested-resize table.
                CAS_new(new NBSI(sz, _size, _non_blocking_set_int));
            }
            // Return self for 'fluid' programming style
            return this;
        }

        // Help any top-level NBSI to copy until completed.
        // Always return the _new version of *this* NBSI, in case we're nested.

        private NBSI help_copy() {
            // Pick some words to help with - but only help copy the top-level NBSI.
            // Nested NBSI waits until the top is done before we start helping.
            NBSI top_nbsi = _non_blocking_set_int._nbsi;
            final int HELP = 8;       // Tuning number: how much copy pain are we willing to inflict?
            // We "help" by forcing individual bit indices to copy.  However, bits
            // come in lumps of 64 per word, so we just advance the bit counter by 64's.
            int idx = top_nbsi._copyIdx.getAndAdd(64 * HELP);
            for (int i = 0; i < HELP; i++) {
                int j = idx + i * 64;
                j %= (top_nbsi._bits.length << 6); // Limit, wrap to array size; means we retry indices
                top_nbsi.help_copy_impl(j);
                top_nbsi.help_copy_impl(j + 63); // Also force the nested-by-64 bit
            }

            // Top level guy ready to promote?
            // Note: WE may not be the top-level guy!
            if (top_nbsi._copyDone.get() == top_nbsi._sum_bits_length)
                // One shot CAS to promote - it may fail since we are racing; others
                // may promote as well
                if (_non_blocking_set_int.CAS_nbsi(top_nbsi, top_nbsi._new)) {
                    //System.out.println("Promote at top level to size "+(_non_blocking_set_int._nbsi._bits.length<<6));
                }

            // Return the new bitvector for 'fluid' programming style
            return _new;
        }

        // Help copy this one word.  State Machine.
        // (1) If not "made immutable" in the old array, set the sign bit to make
        //     it immutable.
        // (2) If non-zero in old array & zero in new, CAS new from 0 to copy-of-old
        // (3) If non-zero in old array & non-zero in new, CAS old to zero
        // (4) Zero in old, new is valid
        // At this point, old should be immutable-zero & new has a copy of bits

        private NBSI help_copy_impl(int i) {
            // Handle every 64th bit via using a nested array
            NBSI old = this;          // The bit array being copied from
            NBSI nnn = _new;          // The bit array being copied to
            if (nnn == null) return this; // Promoted already
            int j = i;                // The bit index being added
            while ((j & 63) == 63) {   // Bit 64? (low 6 bits are all set)
                old = old._nbsi64;      // Recurse
                nnn = nnn._nbsi64;      // Recurse
                j = j >> 6;               // Strip off low 6 bits (all set)
            }

            // Transit from state 1: word is not immutable yet
            // Immutable is in bit 63, the sign bit.
            long bits = old._bits[j >> 6];
            while (bits >= 0) {      // Still in state (1)?
                long oldbits = bits;
                bits |= mask(63);       // Target state of bits: sign-bit means immutable
                if (old.CAS(j >> 6, oldbits, bits)) {
                    if (oldbits == 0) _copyDone.addAndGet(1);
                    break;                // Success - old array word is now immutable
                }
                bits = old._bits[j >> 6]; // Retry if CAS failed
            }

            // Transit from state 2: non-zero in old and zero in new
            if (bits != mask(63)) {  // Non-zero in old?
                long new_bits = nnn._bits[j >> 6];
                if (new_bits == 0) {   // New array is still zero
                    new_bits = bits & ~mask(63); // Desired new value: a mutable copy of bits
                    // One-shot CAS attempt, no loop, from 0 to non-zero.
                    // If it fails, somebody else did the copy for us
                    if (!nnn.CAS(j >> 6, 0, new_bits))
                        new_bits = nnn._bits[j >> 6]; // Since it failed, get the new value
                    assert new_bits != 0;
                }

                // Transit from state 3: non-zero in old and non-zero in new
                // One-shot CAS attempt, no loop, from non-zero to 0 (but immutable)
                if (old.CAS(j >> 6, bits, mask(63)))
                    _copyDone.addAndGet(1); // One more word finished copying
            }

            // Now in state 4: zero (and immutable) in old

            // Return the self bitvector for 'fluid' programming style
            return this;
        }

        private void print(int d, String msg) {
            for (int i = 0; i < d; i++)
                System.out.print("  ");
            System.out.println(msg);
        }

        private void print(int d) {
            StringBuffer buf = new StringBuffer();
            buf.append("NBSI - _bits.len=");
            NBSI x = this;
            while (x != null) {
                buf.append(" " + x._bits.length);
                x = x._nbsi64;
            }
            print(d, buf.toString());

            x = this;
            while (x != null) {
                for (int i = 0; i < x._bits.length; i++)
                    System.out.print(Long.toHexString(x._bits[i]) + " ");
                x = x._nbsi64;
                System.out.println();
            }

            if (_copyIdx.get() != 0 || _copyDone.get() != 0)
                print(d, "_copyIdx=" + _copyIdx.get() + " _copyDone=" + _copyDone.get() + " _words_to_cpy=" + _sum_bits_length);
            if (_new != null) {
                print(d, "__has_new - ");
                _new.print(d + 1);
            }
        }
    }
}
