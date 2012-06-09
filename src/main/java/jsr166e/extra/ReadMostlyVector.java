/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package jsr166e.extra;
import jsr166e.*;
import java.util.*;

/**
 * A class with the same methods and array-based characteristics as
 * {@link java.util.Vector} but with reduced contention and improved
 * throughput when invocations of read-only methods by multiple
 * threads are most common.
 *
 * <p> The iterators returned by this class's {@link #iterator()
 * iterator} and {@link #listIterator(int) listIterator} methods are
 * best-effort in the presence of concurrent modifications, and do
 * <em>NOT</em> throw {@link ConcurrentModificationException}.  An
 * iterator's {@code next()} method returns consecutive elements as
 * they appear in the underlying array upon each access. Alternatively,
 * method {@link #snapshotIterator} may be used for deterministic
 * traversals, at the expense of making a copy, and unavailability of
 * method {@code Iterator.remove}.
 *
 * <p>Otherwise, this class supports all methods, under the same
 * documented specifications, as {@code Vector}.  Consult {@link
 * java.util.Vector} for detailed specifications.  Additionally, this
 * class provides methods {@link #addIfAbsent} and {@link
 * #addAllAbsent}.
 *
 * @author Doug Lea
 */
public class ReadMostlyVector<E>
        implements List<E>, RandomAccess, Cloneable, java.io.Serializable {
    private static final long serialVersionUID = 8673264195747942595L;

    /*
     * This class exists mainly as a vehicle to exercise various
     * constructions using SequenceLocks. Read-only methods
     * take one of a few forms:
     *
     * Short methods,including get(index), continually retry obtaining
     * a snapshot of array, count, and element, using sequence number
     * to validate.
     *
     * Methods that are potentially O(n) (or worse) try once in
     * read-only mode, and then lock. When in read-only mode, they
     * validate only at the end of an array scan unless the element is
     * actually used (for example, as an argument of method equals).
     *
     * We rely on some invariants that are always true, even for field
     * reads in read-only mode that have not yet been validated:
     * - array != null
     * - count >= 0
     */

    /**
     * The maximum size of array to allocate.
     * See CopyOnWriteArrayList for explanation.
     */
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    // fields are non-private to simplify nested class access
    volatile Object[] array;
    final SequenceLock lock;
    volatile int count;
    final int capacityIncrement;

    /**
     * Creates an empty vector with the given initial capacity and
     * capacity increment.
     *
     * @param initialCapacity the initial capacity of the underlying array
     * @param capacityIncrement if non-zero, the number to
     * add when resizing to accommodate additional elements.
     * If zero, the array size is doubled when resized.
     *
     * @throws IllegalArgumentException if initial capacity is negative
     */
    public ReadMostlyVector(int initialCapacity, int capacityIncrement) {
        super();
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal Capacity: "+
                                               initialCapacity);
        this.array = new Object[initialCapacity];
        this.capacityIncrement = capacityIncrement;
        this.lock = new SequenceLock();
    }

    /**
     * Creates an empty vector with the given initial capacity.
     *
     * @param initialCapacity the initial capacity of the underlying array
     *
     * @throws IllegalArgumentException if initial capacity is negative
     */
    public ReadMostlyVector(int initialCapacity) {
        this(initialCapacity, 0);
    }

    /**
     * Creates an empty vector with an underlying array of size {@code 10}.
     */
    public ReadMostlyVector() {
        this(10, 0);
    }

    /**
     * Creates a vector containing the elements of the specified
     * collection, in the order they are returned by the collection's
     * iterator.
     *
     * @param c the collection of initially held elements
     * @throws NullPointerException if the specified collection is null
     */
    public ReadMostlyVector(Collection<? extends E> c) {
        Object[] elements = c.toArray();
        // c.toArray might (incorrectly) not return Object[] (see 6260652)
        if (elements.getClass() != Object[].class)
            elements = Arrays.copyOf(elements, elements.length, Object[].class);
        this.array = elements;
        this.count = elements.length;
        this.capacityIncrement = 0;
        this.lock = new SequenceLock();
    }

    // internal constructor for clone
    ReadMostlyVector(Object[] array, int count, int capacityIncrement) {
        this.array = array;
        this.count = count;
        this.capacityIncrement = capacityIncrement;
        this.lock = new SequenceLock();
    }

    // For explanation, see CopyOnWriteArrayList
    final Object[] grow(int minCapacity) {
        int oldCapacity = array.length;
        int newCapacity = oldCapacity + ((capacityIncrement > 0) ?
                                         capacityIncrement : oldCapacity);
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        return array = Arrays.copyOf(array, newCapacity);
    }

    static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ?
            Integer.MAX_VALUE :
            MAX_ARRAY_SIZE;
    }

    /*
     * Internal versions of most base functionality, wrapped
     * in different ways from public methods from this class
     * as well as sublist and iterator classes.
     */

    /**
     * Version of indexOf that returns -1 if either not present or invalid.
     *
     * @throws ArrayIndexOutOfBoundsException if index is negative
     */
    final int validatedIndexOf(Object x, Object[] items, int index, int fence,
                               long seq) {
        for (int i = index; i < fence; ++i) {
            Object e = items[i];
            if (lock.getSequence() != seq)
                break;
            if ((x == null) ? e == null : x.equals(e))
                return i;
        }
        return -1;
    }

    /**
     * @throws ArrayIndexOutOfBoundsException if index is negative
     */
    final int rawIndexOf(Object x, int index, int fence) {
        Object[] items = array;
        for (int i = index; i < fence; ++i) {
            Object e = items[i];
            if ((x == null) ? e == null : x.equals(e))
                return i;
        }
        return -1;
    }

    final int validatedLastIndexOf(Object x, Object[] items,
                                   int index, int origin, long seq) {
        for (int i = index; i >= origin; --i) {
            Object e = items[i];
            if (lock.getSequence() != seq)
                break;
            if ((x == null) ? e == null : x.equals(e))
                return i;
        }
        return -1;
    }

    final int rawLastIndexOf(Object x, int index, int origin) {
        Object[] items = array;
        for (int i = index; i >= origin; --i) {
            Object e = items[i];
            if ((x == null) ? e == null : x.equals(e))
                return i;
        }
        return -1;
    }

    final void rawAdd(E e) {
        int n = count;
        Object[] items = array;
        if (n >= items.length)
            items = grow(n + 1);
        items[n] = e;
        count = n + 1;
    }

    final void rawAddAt(int index, E e) {
        int n = count;
        Object[] items = array;
        if (index > n)
            throw new ArrayIndexOutOfBoundsException(index);
        if (n >= items.length)
            items = grow(n + 1);
        if (index < n)
            System.arraycopy(items, index, items, index + 1, n - index);
        items[index] = e;
        count = n + 1;
    }

    final boolean rawAddAllAt(int index, Object[] elements) {
        int n = count;
        Object[] items = array;
        if (index < 0 || index > n)
            throw new ArrayIndexOutOfBoundsException(index);
        int len = elements.length;
        if (len == 0)
            return false;
        int newCount = n + len;
        if (newCount >= items.length)
            items = grow(newCount);
        int mv = n - index;
        if (mv > 0)
            System.arraycopy(items, index, items, index + len, mv);
        System.arraycopy(elements, 0, items, index, len);
        count = newCount;
        return true;
    }

    final boolean rawRemoveAt(int index) {
        int n = count - 1;
        Object[] items = array;
        if (index < 0 || index > n)
            return false;
        int mv = n - index;
        if (mv > 0)
            System.arraycopy(items, index + 1, items, index, mv);
        items[n] = null;
        count = n;
        return true;
    }

    /**
     * Internal version of removeAll for lists and sublists. In this
     * and other similar methods below, the bound argument is, if
     * non-negative, the purported upper bound of a list/sublist, or
     * is left negative if the bound should be determined via count
     * field under lock.
     */
    final boolean internalRemoveAll(Collection<?> c, int origin, int bound) {
        final SequenceLock lock = this.lock;
        boolean removed = false;
        lock.lock();
        try {
            int n = count;
            int fence = bound < 0 || bound > n ? n : bound;
            if (origin >= 0 && origin < fence) {
                for (Object x : c) {
                    while (rawRemoveAt(rawIndexOf(x, origin, fence)))
                        removed = true;
                }
            }
        } finally {
            lock.unlock();
        }
        return removed;
    }

    final boolean internalRetainAll(Collection<?> c, int origin, int bound) {
        final SequenceLock lock = this.lock;
        boolean removed = false;
        if (c != this) {
            lock.lock();
            try {
                Object[] items = array;
                int i = origin;
                int n = count;
                int fence = bound < 0 || bound > n ? n : bound;
                while (i >= 0 && i < fence) {
                    if (c.contains(items[i]))
                        ++i;
                    else {
                        --fence;
                        int mv = --n - i;
                        if (mv > 0)
                            System.arraycopy(items, i + 1, items, i, mv);
                    }
                }
                if (count != n) {
                    count = n;
                    removed = true;
                }
            } finally {
                lock.unlock();
            }
        }
        return removed;
    }

    final void internalClear(int origin, int bound) {
        int n = count;
        int fence = bound < 0 || bound > n ? n : bound;
        if (origin >= 0 && origin < fence) {
            Object[] items = array;
            int removed = fence - origin;
            int newCount = n - removed;
            int mv = n - (origin + removed);
            if (mv > 0)
                System.arraycopy(items, origin + removed, items, origin, mv);
            for (int i = n; i < newCount; ++i)
                items[i] = null;
            count = newCount;
        }
    }

    final boolean internalContainsAll(Collection<?> c, int origin, int bound) {
        final SequenceLock lock = this.lock;
        boolean contained;
        boolean locked = false;
        try {
            for (;;) {
                long seq = lock.awaitAvailability();
                int n = count;
                Object[] items = array;
                int len = items.length;
                if (n > len)
                    continue;
                int fence = bound < 0 || bound > n ? n : bound;
                if (origin < 0)
                    contained = false;
                else {
                    contained = true;
                    for (Object e : c) {
                        int idx = (locked ?
                                   rawIndexOf(e, origin, fence) :
                                   validatedIndexOf(e, items, origin,
                                                    fence, seq));
                        if (idx < 0) {
                            contained = false;
                            break;
                        }
                    }
                }
                if (lock.getSequence() == seq)
                    break;
                lock.lock();
                locked = true;
            }
        } finally {
            if (locked)
                lock.unlock();
        }
        return contained;
    }

    final boolean internalEquals(List<?> list, int origin, int bound) {
        final SequenceLock lock = this.lock;
        boolean locked = false;
        boolean equal;
        try {
            for (;;) {
                long seq = lock.awaitAvailability();
                Object[] items = array;
                int n = count;
                if (n > items.length || origin < 0)
                    equal = false;
                else {
                    equal = true;
                    int fence = bound < 0 || bound > n ? n : bound;
                    Iterator<?> it = list.iterator();
                    for (int i = origin; i < fence; ++i) {
                        Object x = items[i];
                        Object y;
                        if ((!locked && lock.getSequence() != seq) ||
                            !it.hasNext() ||
                            (y = it.next()) == null ?
                            x != null : !y.equals(x)) {
                            equal = false;
                            break;
                        }
                    }
                    if (equal && it.hasNext())
                        equal = false;
                }
                if (lock.getSequence() == seq)
                    break;
                lock.lock();
                locked = true;
            }
        } finally {
            if (locked)
                lock.unlock();
        }
        return equal;
    }

    final int internalHashCode(int origin, int bound) {
        final SequenceLock lock = this.lock;
        int hash;
        boolean locked = false;
        try {
            for (;;) {
                hash = 1;
                long seq = lock.awaitAvailability();
                Object[] items = array;
                int n = count;
                int len = items.length;
                if (n > len)
                    continue;
                int fence = bound < 0 || bound > n ? n : bound;
                if (origin >= 0) {
                    for (int i = origin; i < fence; ++i) {
                        Object e = items[i];
                        hash = 31*hash + (e == null ? 0 : e.hashCode());
                    }
                }
                if (lock.getSequence() == seq)
                    break;
                lock.lock();
                locked = true;
            }
        } finally {
            if (locked)
                lock.unlock();
        }
        return hash;
    }

    final String internalToString(int origin, int bound) {
        final SequenceLock lock = this.lock;
        String ret;
        boolean locked = false;
        try {
            outer:for (;;) {
                long seq = lock.awaitAvailability();
                Object[] items = array;
                int n = count;
                int len = items.length;
                if (n > len)
                    continue;
                int fence = bound < 0 || bound > n ? n : bound;
                if (origin < 0 || origin == fence)
                    ret = "[]";
                else {
                    StringBuilder sb = new StringBuilder();
                    sb.append('[');
                    for (int i = origin;;) {
                        Object e = items[i];
                        if (e == this)
                            sb.append("(this Collection)");
                        else if (!locked && lock.getSequence() != seq)
                            continue outer;
                        else
                            sb.append(e.toString());
                        if (++i < fence)
                            sb.append(',').append(' ');
                        else {
                            ret = sb.append(']').toString();
                            break;
                        }
                    }
                }
                if (lock.getSequence() == seq)
                    break;
                lock.lock();
                locked = true;
            }
        } finally {
            if (locked)
                lock.unlock();
        }
        return ret;
    }

    final Object[] internalToArray(int origin, int bound) {
        Object[] result;
        final SequenceLock lock = this.lock;
        boolean locked = false;
        try {
            for (;;) {
                result = null;
                long seq = lock.awaitAvailability();
                Object[] items = array;
                int n = count;
                int len = items.length;
                if (n > len)
                    continue;
                int fence = bound < 0 || bound > n ? n : bound;
                if (origin >= 0)
                    result = Arrays.copyOfRange(items, origin, fence,
                                                Object[].class);
                if (lock.getSequence() == seq)
                    break;
                lock.lock();
                locked = true;
            }
        } finally {
            if (locked)
                lock.unlock();
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    final <T> T[] internalToArray(T[] a, int origin, int bound) {
        int alen = a.length;
        T[] result;
        final SequenceLock lock = this.lock;
        boolean locked = false;
        try {
            for (;;) {
                long seq = lock.awaitAvailability();
                Object[] items = array;
                int n = count;
                int len = items.length;
                if (n > len)
                    continue;
                int fence = bound < 0 || bound > n ? n : bound;
                int rlen = fence - origin;
                if (rlen < 0)
                    rlen = 0;
                if (origin < 0 || alen >= rlen) {
                    if (rlen > 0)
                        System.arraycopy(items, 0, a, origin, rlen);
                    if (alen > rlen)
                        a[rlen] = null;
                    result = a;
                }
                else
                    result = (T[]) Arrays.copyOfRange(items, origin,
                                                      fence, a.getClass());
                if (lock.getSequence() == seq)
                    break;
                lock.lock();
                locked = true;
            }
        } finally {
            if (locked)
                lock.unlock();
        }
        return result;
    }

    // public List methods

    public boolean add(E e) {
        final SequenceLock lock = this.lock;
        lock.lock();
        try {
            rawAdd(e);
        } finally {
            lock.unlock();
        }
        return true;
    }

    public void add(int index, E element) {
        final SequenceLock lock = this.lock;
        lock.lock();
        try {
            rawAddAt(index, element);
        } finally {
            lock.unlock();
        }
    }

    public boolean addAll(Collection<? extends E> c) {
        Object[] elements = c.toArray();
        int len = elements.length;
        if (len == 0)
            return false;
        final SequenceLock lock = this.lock;
        lock.lock();
        try {
            Object[] items = array;
            int n = count;
            int newCount = n + len;
            if (newCount >= items.length)
                items = grow(newCount);
            System.arraycopy(elements, 0, items, n, len);
            count = newCount;
        } finally {
            lock.unlock();
        }
        return true;
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        final SequenceLock lock = this.lock;
        boolean ret;
        Object[] elements = c.toArray();
        lock.lock();
        try {
            ret = rawAddAllAt(index, elements);
        } finally {
            lock.unlock();
        }
        return ret;
    }

    public void clear() {
        final SequenceLock lock = this.lock;
        lock.lock();
        try {
            int n = count;
            Object[] items = array;
            for (int i = 0; i < n; i++)
                items[i] = null;
            count = 0;
        } finally {
            lock.unlock();
        }
    }

    public boolean contains(Object o) {
        return indexOf(o, 0) >= 0;
    }

    public boolean containsAll(Collection<?> c) {
        return internalContainsAll(c, 0, -1);
    }

    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof List))
            return false;
        return internalEquals((List<?>)o, 0, -1);
    }

    public E get(int index) {
        final SequenceLock lock = this.lock;
        for (;;) {
            long seq = lock.awaitAvailability();
            int n = count;
            Object[] items = array;
            @SuppressWarnings("unchecked")
            E e = (index < items.length) ? (E) items[index] : null;
            if (lock.getSequence() == seq) {
                if (index >= n)
                    throw new ArrayIndexOutOfBoundsException(index);
                return e;
            }
        }
    }

    public int hashCode() {
        return internalHashCode(0, -1);
    }

    public int indexOf(Object o) {
        return indexOf(o, 0);
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public Iterator<E> iterator() {
        return new Itr<E>(this, 0);
    }

    public int lastIndexOf(Object o) {
        final SequenceLock lock = this.lock;
        for (;;) {
            long seq = lock.awaitAvailability();
            Object[] items = array;
            int n = count;
            if (n <= items.length) {
                for (int i = n - 1; i >= 0; --i) {
                    Object e = items[i];
                    if (lock.getSequence() != seq) {
                        lock.lock();
                        try {
                            return rawLastIndexOf(o, count - 1, 0);
                        } finally {
                            lock.unlock();
                        }
                    }
                    else if ((o == null) ? e == null : o.equals(e))
                        return i;
                }
                return -1;
            }
        }
    }

    public ListIterator<E> listIterator() {
        return new Itr<E>(this, 0);
    }

    public ListIterator<E> listIterator(int index) {
        return new Itr<E>(this, index);
    }

    public E remove(int index) {
        final SequenceLock lock = this.lock;
        lock.lock();
        try {
            if (index < 0 || index >= count)
                throw new ArrayIndexOutOfBoundsException(index);
            @SuppressWarnings("unchecked")
            E oldValue = (E) array[index];
            rawRemoveAt(index);
            return oldValue;
        } finally {
            lock.unlock();
        }
    }

    public boolean remove(Object o) {
        final SequenceLock lock = this.lock;
        lock.lock();
        try {
            return rawRemoveAt(rawIndexOf(o, 0, count));
        } finally {
            lock.unlock();
        }
    }

    public boolean removeAll(Collection<?> c) {
        return internalRemoveAll(c, 0, -1);
    }

    public boolean retainAll(Collection<?> c) {
        return internalRetainAll(c, 0, -1);
    }

    public E set(int index, E element) {
        final SequenceLock lock = this.lock;
        lock.lock();
        try {
            Object[] items = array;
            if (index < 0 || index >= count)
                throw new ArrayIndexOutOfBoundsException(index);
            @SuppressWarnings("unchecked")
            E oldValue = (E) items[index];
            items[index] = element;
            return oldValue;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        return count;
    }

    public List<E> subList(int fromIndex, int toIndex) {
        int c = size();
        int ssize = toIndex - fromIndex;
        if (fromIndex < 0 || toIndex > c || ssize < 0)
            throw new IndexOutOfBoundsException();
        return new ReadMostlyVectorSublist<E>(this, fromIndex, ssize);
    }

    public Object[] toArray() {
        return internalToArray(0, -1);
    }

    public <T> T[] toArray(T[] a) {
        return internalToArray(a, 0, -1);
    }

    public String toString() {
        return internalToString(0, -1);
    }

    // ReadMostlyVector-only methods

    /**
     * Appends the element, if not present.
     *
     * @param e element to be added to this list, if absent
     * @return {@code true} if the element was added
     */
    public boolean addIfAbsent(E e) {
        final SequenceLock lock = this.lock;
        lock.lock();
        try {
            if (rawIndexOf(e, 0, count) < 0) {
                rawAdd(e);
                return true;
            }
            else
                return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Appends all of the elements in the specified collection that
     * are not already contained in this list, to the end of
     * this list, in the order that they are returned by the
     * specified collection's iterator.
     *
     * @param c collection containing elements to be added to this list
     * @return the number of elements added
     * @throws NullPointerException if the specified collection is null
     * @see #addIfAbsent(Object)
     */
    public int addAllAbsent(Collection<? extends E> c) {
        int added = 0;
        Object[] cs = c.toArray();
        int clen = cs.length;
        if (clen != 0) {
            lock.lock();
            try {
                for (int i = 0; i < clen; ++i) {
                    @SuppressWarnings("unchecked")
                    E e = (E) cs[i];
                    if (rawIndexOf(e, 0, count) < 0) {
                        rawAdd(e);
                        ++added;
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        return added;
    }

    /**
     * Returns an iterator operating over a snapshot copy of the
     * elements of this collection created upon construction of the
     * iterator. The iterator does <em>NOT</em> support the
     * {@code remove} method.
     *
     * @return an iterator over the elements in this list in proper sequence
     */
    public Iterator<E> snapshotIterator() {
        return new SnapshotIterator<E>(this);
    }

    static final class SnapshotIterator<E> implements Iterator<E> {
        private final Object[] items;
        private int cursor;
        SnapshotIterator(ReadMostlyVector<E> v) { items = v.toArray(); }
        public boolean hasNext() { return cursor < items.length; }
        @SuppressWarnings("unchecked")
        public E next() {
            if (cursor < items.length)
                return (E) items[cursor++];
            throw new NoSuchElementException();
        }
        public void remove() { throw new UnsupportedOperationException() ; }
    }

    // Vector-only methods

    /** See {@link Vector#firstElement} */
    public E firstElement() {
        final SequenceLock lock = this.lock;
        for (;;) {
            long seq = lock.awaitAvailability();
            Object[] items = array;
            int n = count;
            @SuppressWarnings("unchecked")
            E e = (items.length > 0) ? (E) items[0] : null;
            if (lock.getSequence() == seq) {
                if (n <= 0)
                    throw new NoSuchElementException();
                return e;
            }
        }
    }

    /** See {@link Vector#lastElement} */
    public E lastElement() {
        final SequenceLock lock = this.lock;
        for (;;) {
            long seq = lock.awaitAvailability();
            Object[] items = array;
            int n = count;
            @SuppressWarnings("unchecked")
            E e = (n > 0 && items.length >= n) ? (E) items[n - 1] : null;
            if (lock.getSequence() == seq) {
                if (n <= 0)
                    throw new NoSuchElementException();
                return e;
            }
        }
    }

    /** See {@link Vector#indexOf(Object, int)} */
    public int indexOf(Object o, int index) {
        final SequenceLock lock = this.lock;
        long seq = lock.awaitAvailability();
        Object[] items = array;
        int n = count;
        int idx = -1;
        if (n <= items.length)
            idx = validatedIndexOf(o, items, index, n, seq);
        if (lock.getSequence() != seq) {
            lock.lock();
            try {
                idx = rawIndexOf(o, index, count);
            } finally {
                lock.unlock();
            }
        }
        // Above code will throw AIOOBE when index < 0
        return idx;
    }

    /** See {@link Vector#lastIndexOf(Object, int)} */
    public int lastIndexOf(Object o, int index) {
        final SequenceLock lock = this.lock;
        long seq = lock.awaitAvailability();
        Object[] items = array;
        int n = count;
        int idx = -1;
        if (index < Math.min(n, items.length))
            idx = validatedLastIndexOf(o, items, index, 0, seq);
        if (lock.getSequence() != seq) {
            lock.lock();
            try {
                n = count;
                if (index < n)
                    idx = rawLastIndexOf(o, index, 0);
            } finally {
                lock.unlock();
            }
        }
        if (index >= n)
            throw new IndexOutOfBoundsException(index + " >= " + n);
        return idx;
    }

    /** See {@link Vector#setSize} */
    public void setSize(int newSize) {
        if (newSize < 0)
            throw new ArrayIndexOutOfBoundsException(newSize);
        final SequenceLock lock = this.lock;
        lock.lock();
        try {
            int n = count;
            if (newSize > n)
                grow(newSize);
            else {
                Object[] items = array;
                for (int i = newSize ; i < n ; i++)
                    items[i] = null;
            }
            count = newSize;
        } finally {
            lock.unlock();
        }
    }

    /** See {@link Vector#copyInto} */
    public void copyInto(Object[] anArray) {
        final SequenceLock lock = this.lock;
        lock.lock();
        try {
            System.arraycopy(array, 0, anArray, 0, count);
        } finally {
            lock.unlock();
        }
    }

    /** See {@link Vector#trimToSize} */
    public void trimToSize() {
        final SequenceLock lock = this.lock;
        lock.lock();
        try {
            Object[] items = array;
            int n = count;
            if (n < items.length)
                array = Arrays.copyOf(items, n);
        } finally {
            lock.unlock();
        }
    }

    /** See {@link Vector#ensureCapacity} */
    public void ensureCapacity(int minCapacity) {
        if (minCapacity > 0) {
            final SequenceLock lock = this.lock;
            lock.lock();
            try {
                if (minCapacity - array.length > 0)
                    grow(minCapacity);
            } finally {
                lock.unlock();
            }
        }
    }

    /** See {@link Vector#elements} */
    public Enumeration<E> elements() {
        return new Itr<E>(this, 0);
    }

    /** See {@link Vector#capacity} */
    public int capacity() {
        return array.length;
    }

    /** See {@link Vector#elementAt} */
    public E elementAt(int index) {
        return get(index);
    }

    /** See {@link Vector#setElementAt} */
    public void setElementAt(E obj, int index) {
        set(index, obj);
    }

    /** See {@link Vector#removeElementAt} */
    public void removeElementAt(int index) {
        remove(index);
    }

    /** See {@link Vector#insertElementAt} */
    public void insertElementAt(E obj, int index) {
        add(index, obj);
    }

    /** See {@link Vector#addElement} */
    public void addElement(E obj) {
        add(obj);
    }

    /** See {@link Vector#removeElement} */
    public boolean removeElement(Object obj) {
        return remove(obj);
    }

    /** See {@link Vector#removeAllElements} */
    public void removeAllElements() {
        clear();
    }

    // other methods

    public ReadMostlyVector<E> clone() {
        final SequenceLock lock = this.lock;
        Object[] a = null;
        boolean retry = false;
        long seq = lock.awaitAvailability();
        Object[] items = array;
        int n = count;
        if (n <= items.length)
            a = Arrays.copyOf(items, n);
        else
            retry = true;
        if (retry || lock.getSequence() != seq) {
            lock.lock();
            try {
                n = count;
                a = Arrays.copyOf(array, n);
            } finally {
                lock.unlock();
            }
        }
        return new ReadMostlyVector<E>(a, n, capacityIncrement);
    }

    private void writeObject(java.io.ObjectOutputStream s)
            throws java.io.IOException {
        final SequenceLock lock = this.lock;
        lock.lock();
        try {
            s.defaultWriteObject();
        } finally {
            lock.unlock();
        }
    }

    static final class Itr<E> implements ListIterator<E>, Enumeration<E> {
        final ReadMostlyVector<E> list;
        final SequenceLock lock;
        Object[] items;
        E next, prev;
        long seq;
        int cursor;
        int fence;
        int lastRet;
        boolean validNext, validPrev;

        Itr(ReadMostlyVector<E> list, int index) {
            this.list = list;
            this.lock = list.lock;
            this.cursor = index;
            this.lastRet = -1;
            refresh();
            if (index < 0 || index > fence)
                throw new ArrayIndexOutOfBoundsException(index);
        }

        private void refresh() {
            validNext = validPrev = false;
            do {
                seq = lock.awaitAvailability();
                items = list.array;
            } while ((fence = list.count) > items.length ||
                     lock.getSequence() != seq);
        }

        @SuppressWarnings("unchecked")
        public boolean hasNext() {
            boolean valid;
            int i = cursor;
            for (;;) {
                if (i >= fence || i < 0 || i >= items.length) {
                    valid = false;
                    break;
                }
                next = (E) items[i];
                if (lock.getSequence() == seq) {
                    valid = true;
                    break;
                }
                refresh();
            }
            return validNext = valid;
        }

        @SuppressWarnings("unchecked")
        public boolean hasPrevious() {
            boolean valid;
            int i = cursor - 1;
            for (;;) {
                if (i >= fence || i < 0 || i >= items.length) {
                    valid = false;
                    break;
                }
                prev = (E) items[i];
                if (lock.getSequence() == seq) {
                    valid = true;
                    break;
                }
                refresh();
            }
            return validPrev = valid;
        }

        public E next() {
            if (validNext || hasNext()) {
                validNext = false;
                lastRet = cursor++;
                return next;
            }
            throw new NoSuchElementException();
        }

        public E previous() {
            if (validPrev || hasPrevious()) {
                validPrev = false;
                lastRet = cursor--;
                return prev;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            int i = lastRet;
            if (i < 0)
                throw new IllegalStateException();
            lock.lock();
            try {
                if (i < list.count)
                    list.remove(i);
            } finally {
                lock.unlock();
            }
            cursor = i;
            lastRet = -1;
            refresh();
        }

        public void set(E e) {
            int i = lastRet;
            if (i < 0)
                throw new IllegalStateException();
            lock.lock();
            try {
                if (i < list.count)
                    list.set(i, e);
            } finally {
                lock.unlock();
            }
            refresh();
        }

        public void add(E e) {
            int i = cursor;
            if (i < 0)
                throw new IllegalStateException();
            lock.lock();
            try {
                if (i <= list.count)
                    list.add(i, e);
            } finally {
                lock.unlock();
            }
            cursor = i + 1;
            lastRet = -1;
            refresh();
        }

        public boolean hasMoreElements() { return hasNext(); }
        public E nextElement() { return next(); }
        public int nextIndex() { return cursor; }
        public int previousIndex() { return cursor - 1; }
    }

    static final class ReadMostlyVectorSublist<E>
            implements List<E>, RandomAccess, java.io.Serializable {
        static final long serialVersionUID = 3041673470172026059L;

        final ReadMostlyVector<E> list;
        final int offset;
        volatile int size;

        ReadMostlyVectorSublist(ReadMostlyVector<E> list,
                                int offset, int size) {
            this.list = list;
            this.offset = offset;
            this.size = size;
        }

        private void rangeCheck(int index) {
            if (index < 0 || index >= size)
                throw new ArrayIndexOutOfBoundsException(index);
        }

        public boolean add(E element) {
            final SequenceLock lock = list.lock;
            lock.lock();
            try {
                int c = size;
                list.rawAddAt(c + offset, element);
                size = c + 1;
            } finally {
                lock.unlock();
            }
            return true;
        }

        public void add(int index, E element) {
            final SequenceLock lock = list.lock;
            lock.lock();
            try {
                if (index < 0 || index > size)
                    throw new ArrayIndexOutOfBoundsException(index);
                list.rawAddAt(index + offset, element);
                ++size;
            } finally {
                lock.unlock();
            }
        }

        public boolean addAll(Collection<? extends E> c) {
            Object[] elements = c.toArray();
            final SequenceLock lock = list.lock;
            lock.lock();
            try {
                int s = size;
                int pc = list.count;
                list.rawAddAllAt(offset + s, elements);
                int added = list.count - pc;
                size = s + added;
                return added != 0;
            } finally {
                lock.unlock();
            }
        }

        public boolean addAll(int index, Collection<? extends E> c) {
            Object[] elements = c.toArray();
            final SequenceLock lock = list.lock;
            lock.lock();
            try {
                int s = size;
                if (index < 0 || index > s)
                    throw new ArrayIndexOutOfBoundsException(index);
                int pc = list.count;
                list.rawAddAllAt(index + offset, elements);
                int added = list.count - pc;
                size = s + added;
                return added != 0;
            } finally {
                lock.unlock();
            }
        }

        public void clear() {
            final SequenceLock lock = list.lock;
            lock.lock();
            try {
                list.internalClear(offset, offset + size);
                size = 0;
            } finally {
                lock.unlock();
            }
        }

        public boolean contains(Object o) {
            return indexOf(o) >= 0;
        }

        public boolean containsAll(Collection<?> c) {
            return list.internalContainsAll(c, offset, offset + size);
        }

        public boolean equals(Object o) {
            if (o == this)
                return true;
            if (!(o instanceof List))
                return false;
            return list.internalEquals((List<?>)(o), offset, offset + size);
        }

        public E get(int index) {
            if (index < 0 || index >= size)
                throw new ArrayIndexOutOfBoundsException(index);
            return list.get(index + offset);
        }

        public int hashCode() {
            return list.internalHashCode(offset, offset + size);
        }

        public int indexOf(Object o) {
            final SequenceLock lock = list.lock;
            long seq = lock.awaitAvailability();
            Object[] items = list.array;
            int c = list.count;
            if (c <= items.length) {
                int idx = list.validatedIndexOf(o, items, offset,
                                                offset + size, seq);
                if (lock.getSequence() == seq)
                    return idx < 0 ? -1 : idx - offset;
            }
            lock.lock();
            try {
                int idx = list.rawIndexOf(o, offset, offset + size);
                return idx < 0 ? -1 : idx - offset;
            } finally {
                lock.unlock();
            }
        }

        public boolean isEmpty() {
            return size == 0;
        }

        public Iterator<E> iterator() {
            return new SubItr<E>(this, offset);
        }

        public int lastIndexOf(Object o) {
            final SequenceLock lock = list.lock;
            long seq = lock.awaitAvailability();
            Object[] items = list.array;
            int c = list.count;
            if (c <= items.length) {
                int idx = list.validatedLastIndexOf(o, items, offset+size-1,
                                                    offset, seq);
                if (lock.getSequence() == seq)
                    return idx < 0 ? -1 : idx - offset;
            }
            lock.lock();
            try {
                int idx = list.rawLastIndexOf(o, offset + size - 1, offset);
                return idx < 0 ? -1 : idx - offset;
            } finally {
                lock.unlock();
            }
        }

        public ListIterator<E> listIterator() {
            return new SubItr<E>(this, offset);
        }

        public ListIterator<E> listIterator(int index) {
            return new SubItr<E>(this, index + offset);
        }

        public E remove(int index) {
            final SequenceLock lock = list.lock;
            lock.lock();
            try {
                Object[] items = list.array;
                int i = index + offset;
                if (index < 0 || index >= size || i >= items.length)
                    throw new ArrayIndexOutOfBoundsException(index);
                @SuppressWarnings("unchecked")
                E result = (E) items[i];
                list.rawRemoveAt(i);
                size--;
                return result;
            } finally {
                lock.unlock();
            }
        }

        public boolean remove(Object o) {
            final SequenceLock lock = list.lock;
            lock.lock();
            try {
                if (list.rawRemoveAt(list.rawIndexOf(o, offset,
                                                     offset + size))) {
                    --size;
                    return true;
                }
                else
                    return false;
            } finally {
                lock.unlock();
            }
        }

        public boolean removeAll(Collection<?> c) {
            return list.internalRemoveAll(c, offset, offset + size);
        }

        public boolean retainAll(Collection<?> c) {
            return list.internalRetainAll(c, offset, offset + size);
        }

        public E set(int index, E element) {
            if (index < 0 || index >= size)
                throw new ArrayIndexOutOfBoundsException(index);
            return list.set(index+offset, element);
        }

        public int size() {
            return size;
        }

        public List<E> subList(int fromIndex, int toIndex) {
            int c = size;
            int ssize = toIndex - fromIndex;
            if (fromIndex < 0 || toIndex > c || ssize < 0)
                throw new IndexOutOfBoundsException();
            return new ReadMostlyVectorSublist<E>(list, offset+fromIndex, ssize);
        }

        public Object[] toArray() {
            return list.internalToArray(offset, offset + size);
        }

        public <T> T[] toArray(T[] a) {
            return list.internalToArray(a, offset, offset + size);
        }

        public String toString() {
            return list.internalToString(offset, offset + size);
        }

    }

    static final class SubItr<E> implements ListIterator<E> {
        final ReadMostlyVectorSublist<E> sublist;
        final ReadMostlyVector<E> list;
        final SequenceLock lock;
        Object[] items;
        E next, prev;
        long seq;
        int cursor;
        int fence;
        int lastRet;
        boolean validNext, validPrev;

        SubItr(ReadMostlyVectorSublist<E> sublist, int index) {
            this.sublist = sublist;
            this.list = sublist.list;
            this.lock = list.lock;
            this.cursor = index;
            this.lastRet = -1;
            refresh();
            if (index < 0 || index > fence)
                throw new ArrayIndexOutOfBoundsException(index);
        }

        private void refresh() {
            validNext = validPrev = false;
            do {
                int n;
                seq = lock.awaitAvailability();
                items = list.array;
                if ((n = list.count) > items.length)
                    continue;
                int b = sublist.offset + sublist.size;
                fence = b < n ? b : n;
            } while (lock.getSequence() != seq);
        }

        @SuppressWarnings("unchecked")
        public boolean hasNext() {
            boolean valid;
            int i = cursor;
            for (;;) {
                if (i >= fence || i < 0 || i >= items.length) {
                    valid = false;
                    break;
                }
                next = (E) items[i];
                if (lock.getSequence() == seq) {
                    valid = true;
                    break;
                }
                refresh();
            }
            return validNext = valid;
        }

        @SuppressWarnings("unchecked")
        public boolean hasPrevious() {
            boolean valid;
            int i = cursor - 1;
            for (;;) {
                if (i >= fence || i < 0 || i >= items.length) {
                    valid = false;
                    break;
                }
                prev = (E) items[i];
                if (lock.getSequence() == seq) {
                    valid = true;
                    break;
                }
                refresh();
            }
            return validPrev = valid;
        }

        public E next() {
            if (validNext || hasNext()) {
                validNext = false;
                lastRet = cursor++;
                return next;
            }
            throw new NoSuchElementException();
        }

        public E previous() {
            if (validPrev || hasPrevious()) {
                validPrev = false;
                lastRet = cursor--;
                return prev;
            }
            throw new NoSuchElementException();
        }

        public int nextIndex() {
            return cursor - sublist.offset;
        }

        public int previousIndex() {
            return cursor - 1 - sublist.offset;
        }

        public void remove() {
            int i = lastRet;
            if (i < 0)
                throw new IllegalStateException();
            cursor = i;
            lastRet = -1;
            lock.lock();
            try {
                if (i < list.count) {
                    list.remove(i);
                    --sublist.size;
                }
            } finally {
                lock.unlock();
            }
            refresh();
        }

        public void set(E e) {
            int i = lastRet;
            if (i < 0)
                throw new IllegalStateException();
            lock.lock();
            try {
                if (i < list.count)
                    list.set(i, e);
            } finally {
                lock.unlock();
            }
            refresh();
        }

        public void add(E e) {
            int i = cursor;
            if (i < 0)
                throw new IllegalStateException();
            cursor = i + 1;
            lastRet = -1;
            lock.lock();
            try {
                if (i <= list.count) {
                    list.add(i, e);
                    ++sublist.size;
                }
            } finally {
                lock.unlock();
            }
            refresh();
        }

    }
}

