/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package jsr166e.extra;
import jsr166e.StampedLock;
import java.util.*;

/**
 * A class with the same methods and array-based characteristics as
 * {@link java.util.Vector} but with reduced contention and improved
 * throughput when invocations of read-only methods by multiple
 * threads are most common.
 *
 * <p>The iterators returned by this class's {@link #iterator()
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
    Object[] array;
    final StampedLock lock;
    int count;
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
        this.lock = new StampedLock();
    }

    /**
     * Creates an empty vector with the given initial capacity.
     *
     * @param initialCapacity the initial capacity of the underlying array
     * @throws IllegalArgumentException if initial capacity is negative
     */
    public ReadMostlyVector(int initialCapacity) {
        this(initialCapacity, 0);
    }

    /**
     * Creates an empty vector.
     */
    public ReadMostlyVector() {
        this.capacityIncrement = 0;
        this.lock = new StampedLock();
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
        this.lock = new StampedLock();
    }

    // internal constructor for clone
    ReadMostlyVector(Object[] array, int count, int capacityIncrement) {
        this.array = array;
        this.count = count;
        this.capacityIncrement = capacityIncrement;
        this.lock = new StampedLock();
    }

    static final int INITIAL_CAP = 16;

    // For explanation, see CopyOnWriteArrayList
    final Object[] grow(int minCapacity) {
        Object[] items;
        int newCapacity;
        if ((items = array) == null)
            newCapacity = INITIAL_CAP;
        else {
            int oldCapacity = array.length;
            newCapacity = oldCapacity + ((capacityIncrement > 0) ?
                                         capacityIncrement : oldCapacity);
        }
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity - MAX_ARRAY_SIZE > 0) {
            if (minCapacity < 0) // overflow
                throw new OutOfMemoryError();
            else if (minCapacity > MAX_ARRAY_SIZE)
                newCapacity = Integer.MAX_VALUE;
            else
                newCapacity = MAX_ARRAY_SIZE;
        }
        return array = ((items == null) ?
                        new Object[newCapacity] :
                        Arrays.copyOf(items, newCapacity));
    }

    /*
     * Internal versions of most base functionality, wrapped
     * in different ways from public methods from this class
     * as well as sublist and iterator classes.
     */

    static int findFirstIndex(Object[] items, Object x, int index, int fence) {
        int len;
        if (items != null && (len = items.length) > 0) {
            int start = (index < 0) ? 0 : index;
            int bound = (fence < len) ? fence : len;
            for (int i = start; i < bound; ++i) {
                Object e = items[i];
                if ((x == null) ? e == null : x.equals(e))
                    return i;
            }
        }
        return -1;
    }

    static int findLastIndex(Object[] items, Object x, int index, int origin) {
        int len;
        if (items != null && (len = items.length) > 0) {
            int last = (index < len) ? index : len - 1;
            int start = (origin < 0) ? 0 : origin;
            for (int i = last; i >= start; --i) {
                Object e = items[i];
                if ((x == null) ? e == null : x.equals(e))
                    return i;
            }
        }
        return -1;
    }

    final void rawAdd(E e) {
        int n = count;
        Object[] items = array;
        if (items == null || n >= items.length)
            items = grow(n + 1);
        items[n] = e;
        count = n + 1;
    }

    final void rawAddAt(int index, E e) {
        int n = count;
        Object[] items = array;
        if (index > n)
            throw new ArrayIndexOutOfBoundsException(index);
        if (items == null || n >= items.length)
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
        if (items == null || newCount >= items.length)
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
        if (items == null || index < 0 || index > n)
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
    final boolean lockedRemoveAll(Collection<?> c, int origin, int bound) {
        boolean removed = false;
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            int n = count;
            int fence = bound < 0 || bound > n ? n : bound;
            if (origin >= 0 && origin < fence) {
                for (Object x : c) {
                    while (rawRemoveAt(findFirstIndex(array, x, origin, fence)))
                        removed = true;
                }
            }
        } finally {
            lock.unlockWrite(stamp);
        }
        return removed;
    }

    final boolean lockedRetainAll(Collection<?> c, int origin, int bound) {
        final StampedLock lock = this.lock;
        boolean removed = false;
        if (c != this) {
            long stamp = lock.writeLock();
            try {
                Object[] items;
                int i, n;
                if ((items = array) != null && (n = count) > 0 &&
                    n < items.length && (i = origin) >= 0) {
                    int fence = bound < 0 || bound > n ? n : bound;
                    while (i < fence) {
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
                }
            } finally {
                lock.unlockWrite(stamp);
            }
        }
        return removed;
    }

    final void internalClear(int origin, int bound) {
        Object[] items;
        int n, len;
        if ((items = array) != null && (len = items.length) > 0) {
            if (origin < 0)
                origin = 0;
            if ((n = count) > len)
                n = len;
            int fence = bound < 0 || bound > n ? n : bound;
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
        Object[] items;
        int n, len;
        if ((items = array) != null && (len = items.length) > 0) {
            if (origin < 0)
                origin = 0;
            if ((n = count) > len)
                n = len;
            int fence = bound < 0 || bound > n ? n : bound;
            for (Object e : c) {
                if (findFirstIndex(items, e, origin, fence) < 0)
                    return false;
            }
        }
        else if (!c.isEmpty())
            return false;
        return true;
    }

    final boolean internalEquals(List<?> list, int origin, int bound) {
        Object[] items;
        int n, len;
        if ((items = array) != null && (len = items.length) > 0) {
            if (origin < 0)
                origin = 0;
            if ((n = count) > len)
                n = len;
            int fence = bound < 0 || bound > n ? n : bound;
            Iterator<?> it = list.iterator();
            for (int i = origin; i < fence; ++i) {
                if (!it.hasNext())
                    return false;
                Object y = it.next();
                Object x = items[i];
                if (x != y && (x == null || !x.equals(y)))
                    return false;
            }
            if (it.hasNext())
                return false;
        }
        else if (!list.isEmpty())
            return false;
        return true;
    }

    final int internalHashCode(int origin, int bound) {
        int hash = 1;
        Object[] items;
        int n, len;
        if ((items = array) != null && (len = items.length) > 0) {
            if (origin < 0)
                origin = 0;
            if ((n = count) > len)
                n = len;
            int fence = bound < 0 || bound > n ? n : bound;
            for (int i = origin; i < fence; ++i) {
                Object e = items[i];
                hash = 31*hash + (e == null ? 0 : e.hashCode());
            }
        }
        return hash;
    }

    final String internalToString(int origin, int bound) {
        Object[] items;
        int n, len;
        if ((items = array) != null && (len = items.length) > 0) {
            if ((n = count) > len)
                n = len;
            int fence = bound < 0 || bound > n ? n : bound;
            int i = (origin < 0) ? 0 : origin;
            if (i != fence) {
                StringBuilder sb = new StringBuilder();
                sb.append('[');
                for (;;) {
                    Object e = items[i];
                    sb.append((e == this) ? "(this Collection)" : e.toString());
                    if (++i < fence)
                        sb.append(',').append(' ');
                    else
                        return sb.append(']').toString();
                }
            }
        }
        return "[]";
    }

    final Object[] internalToArray(int origin, int bound) {
        Object[] items;
        int n, len;
        if ((items = array) != null && (len = items.length) > 0) {
            if (origin < 0)
                origin = 0;
            if ((n = count) > len)
                n = len;
            int fence = bound < 0 || bound > n ? n : bound;
            int i = (origin < 0) ? 0 : origin;
            if (i != fence)
                return Arrays.copyOfRange(items, i, fence, Object[].class);
        }
        return new Object[0];
    }

    @SuppressWarnings("unchecked")
    final <T> T[] internalToArray(T[] a, int origin, int bound) {
        int alen = a.length;
        Object[] items;
        int n, len;
        if ((items = array) != null && (len = items.length) > 0) {
            if (origin < 0)
                origin = 0;
            if ((n = count) > len)
                n = len;
            int fence = bound < 0 || bound > n ? n : bound;
            int i = (origin < 0) ? 0 : origin;
            int rlen = fence - origin;
            if (rlen > 0) {
                if (alen >= rlen) {
                    System.arraycopy(items, 0, a, origin, rlen);
                    if (alen > rlen)
                        a[rlen] = null;
                    return a;
                }
                return (T[]) Arrays.copyOfRange(items, i, fence, a.getClass());
            }
        }
        if (alen > 0)
            a[0] = null;
        return a;
    }

    // public List methods

    public boolean add(E e) {
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            rawAdd(e);
        } finally {
            lock.unlockWrite(stamp);
        }
        return true;
    }

    public void add(int index, E element) {
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            rawAddAt(index, element);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public boolean addAll(Collection<? extends E> c) {
        Object[] elements = c.toArray();
        int len = elements.length;
        if (len == 0)
            return false;
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            Object[] items = array;
            int n = count;
            int newCount = n + len;
            if (items == null || newCount >= items.length)
                items = grow(newCount);
            System.arraycopy(elements, 0, items, n, len);
            count = newCount;
        } finally {
            lock.unlockWrite(stamp);
        }
        return true;
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        Object[] elements = c.toArray();
        boolean ret;
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            ret = rawAddAllAt(index, elements);
        } finally {
            lock.unlockWrite(stamp);
        }
        return ret;
    }

    public void clear() {
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            int n = count;
            Object[] items = array;
            if (items != null) {
                for (int i = 0; i < n; i++)
                    items[i] = null;
            }
            count = 0;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public boolean contains(Object o) {
        return indexOf(o, 0) >= 0;
    }

    public boolean containsAll(Collection<?> c) {
        boolean ret;
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            ret = internalContainsAll(c, 0, -1);
        } finally {
            lock.unlockRead(stamp);
        }
        return ret;
    }

    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof List))
            return false;
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            return internalEquals((List<?>)o, 0, -1);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public E get(int index) {
        final StampedLock lock = this.lock;
        long stamp = lock.tryOptimisticRead();
        Object[] items;
        if (index >= 0 && (items = array) != null &&
            index < count && index < items.length) {
            @SuppressWarnings("unchecked") E e = (E)items[index];
            if (lock.validate(stamp))
                return e;
        }
        return lockedGet(index);
    }

    @SuppressWarnings("unchecked") private E lockedGet(int index) {
        boolean oobe = false;
        E e = null;
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            Object[] items;
            if ((items = array) != null && index < items.length &&
                index < count && index >= 0)
                e = (E)items[index];
            else
                oobe = true;
        } finally {
            lock.unlockRead(stamp);
        }
        if (oobe)
            throw new ArrayIndexOutOfBoundsException(index);
        return e;
    }

    public int hashCode() {
        int h;
        final StampedLock lock = this.lock;
        long s = lock.readLock();
        try {
            h = internalHashCode(0, -1);
        } finally {
            lock.unlockRead(s);
        }
        return h;
    }

    public int indexOf(Object o) {
        int idx;
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            idx = findFirstIndex(array, o, 0, count);
        } finally {
            lock.unlockRead(stamp);
        }
        return idx;
    }

    public boolean isEmpty() {
        final StampedLock lock = this.lock;
        long stamp = lock.tryOptimisticRead();
        return count == 0; // no need for validation
    }

    public Iterator<E> iterator() {
        return new Itr<E>(this, 0);
    }

    public int lastIndexOf(Object o) {
        int idx;
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            idx = findLastIndex(array, o, count - 1, 0);
        } finally {
            lock.unlockRead(stamp);
        }
        return idx;
    }

    public ListIterator<E> listIterator() {
        return new Itr<E>(this, 0);
    }

    public ListIterator<E> listIterator(int index) {
        return new Itr<E>(this, index);
    }

    @SuppressWarnings("unchecked") public E remove(int index) {
        E oldValue = null;
        boolean oobe = false;
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            if (index < 0 || index >= count)
                oobe = true;
            else {
                oldValue = (E) array[index];
                rawRemoveAt(index);
            }
        } finally {
            lock.unlockWrite(stamp);
        }
        if (oobe)
            throw new ArrayIndexOutOfBoundsException(index);
        return oldValue;
    }

    public boolean remove(Object o) {
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            return rawRemoveAt(findFirstIndex(array, o, 0, count));
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public boolean removeAll(Collection<?> c) {
        return lockedRemoveAll(c, 0, -1);
    }

    public boolean retainAll(Collection<?> c) {
        return lockedRetainAll(c, 0, -1);
    }

    @SuppressWarnings("unchecked") public E set(int index, E element) {
        E oldValue = null;
        boolean oobe = false;
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            Object[] items = array;
            if (items == null || index < 0 || index >= count)
                oobe = true;
            else {
                oldValue = (E) items[index];
                items[index] = element;
            }
        } finally {
            lock.unlockWrite(stamp);
        }
        if (oobe)
            throw new ArrayIndexOutOfBoundsException(index);
        return oldValue;
    }

    public int size() {
        final StampedLock lock = this.lock;
        long stamp = lock.tryOptimisticRead();
        return count; // no need for validation
    }

    private int lockedSize() {
        int n;
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            n = count;
        } finally {
            lock.unlockRead(stamp);
        }
        return n;
    }

    public List<E> subList(int fromIndex, int toIndex) {
        int ssize = toIndex - fromIndex;
        if (ssize >= 0 && fromIndex >= 0) {
            ReadMostlyVectorSublist<E> ret = null;
            final StampedLock lock = this.lock;
            long stamp = lock.readLock();
            try {
                if (toIndex <= count)
                    ret = new ReadMostlyVectorSublist<E>(this, fromIndex, ssize);
            } finally {
                lock.unlockRead(stamp);
            }
            if (ret != null)
                return ret;
        }

        throw new ArrayIndexOutOfBoundsException(fromIndex < 0 ? fromIndex : toIndex);
    }

    public Object[] toArray() {
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            return internalToArray(0, -1);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public <T> T[] toArray(T[] a) {
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            return internalToArray(a, 0, -1);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public String toString() {
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            return internalToString(0, -1);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    // ReadMostlyVector-only methods

    /**
     * Appends the element, if not present.
     *
     * @param e element to be added to this list, if absent
     * @return {@code true} if the element was added
     */
    public boolean addIfAbsent(E e) {
        boolean ret;
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            if (findFirstIndex(array, e, 0, count) < 0) {
                rawAdd(e);
                ret = true;
            }
            else
                ret = false;
        } finally {
            lock.unlockWrite(stamp);
        }
        return ret;
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
            long stamp = lock.writeLock();
            try {
                for (int i = 0; i < clen; ++i) {
                    @SuppressWarnings("unchecked")
                    E e = (E) cs[i];
                    if (findFirstIndex(array, e, 0, count) < 0) {
                        rawAdd(e);
                        ++added;
                    }
                }
            } finally {
                lock.unlockWrite(stamp);
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
        @SuppressWarnings("unchecked") public E next() {
            if (cursor < items.length)
                return (E) items[cursor++];
            throw new NoSuchElementException();
        }
        public void remove() { throw new UnsupportedOperationException() ; }
    }

    /** Interface describing a void action of one argument */
    public interface Action<A> { void apply(A a); }

    public void forEachReadOnly(Action<E> action) {
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            Object[] items;
            int len, n;
            if ((items = array) != null && (len = items.length) > 0 &&
                (n = count) <= len) {
                for (int i = 0; i < n; ++i) {
                    @SuppressWarnings("unchecked") E e = (E)items[i];
                    action.apply(e);
                }
            }
        } finally {
            lock.unlockRead(stamp);
        }
    }

    // Vector-only methods

    /** See {@link Vector#firstElement} */
    public E firstElement() {
        final StampedLock lock = this.lock;
        long stamp = lock.tryOptimisticRead();
        Object[] items;
        if ((items = array) != null && count > 0 && items.length > 0) {
            @SuppressWarnings("unchecked") E e = (E)items[0];
            if (lock.validate(stamp))
                return e;
        }
        return lockedFirstElement();
    }

    @SuppressWarnings("unchecked") private E lockedFirstElement() {
        Object e = null;
        boolean oobe = false;
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            Object[] items = array;
            if (items != null && count > 0 && items.length > 0)
                e = items[0];
            else
                oobe = true;
        } finally {
            lock.unlockRead(stamp);
        }
        if (oobe)
            throw new NoSuchElementException();
        return (E) e;
    }

    /** See {@link Vector#lastElement} */
    public E lastElement() {
        final StampedLock lock = this.lock;
        long stamp = lock.tryOptimisticRead();
        Object[] items;
        int i;
        if ((items = array) != null && (i = count - 1) >= 0 &&
            i < items.length) {
            @SuppressWarnings("unchecked") E e = (E)items[i];
            if (lock.validate(stamp))
                return e;
        }
        return lockedLastElement();
    }

    @SuppressWarnings("unchecked") private E lockedLastElement() {
        Object e = null;
        boolean oobe = false;
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            Object[] items = array;
            int i = count - 1;
            if (items != null && i >= 0 && i < items.length)
                e = items[i];
            else
                oobe = true;
        } finally {
            lock.unlockRead(stamp);
        }
        if (oobe)
            throw new NoSuchElementException();
        return (E) e;
    }

    /** See {@link Vector#indexOf(Object, int)} */
    public int indexOf(Object o, int index) {
        if (index < 0)
            throw new ArrayIndexOutOfBoundsException(index);
        int idx;
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            idx = findFirstIndex(array, o, index, count);
        } finally {
            lock.unlockRead(stamp);
        }
        return idx;
    }

    /** See {@link Vector#lastIndexOf(Object, int)} */
    public int lastIndexOf(Object o, int index) {
        boolean oobe = false;
        int idx = -1;
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            if (index < count)
                idx = findLastIndex(array, o, index, 0);
            else
                oobe = true;
        } finally {
            lock.unlockRead(stamp);
        }
        if (oobe)
            throw new ArrayIndexOutOfBoundsException(index);
        return idx;
    }

    /** See {@link Vector#setSize} */
    public void setSize(int newSize) {
        if (newSize < 0)
            throw new ArrayIndexOutOfBoundsException(newSize);
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            Object[] items;
            int n = count;
            if (newSize > n)
                grow(newSize);
            else if ((items = array) != null) {
                for (int i = newSize ; i < n ; i++)
                    items[i] = null;
            }
            count = newSize;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /** See {@link Vector#copyInto} */
    public void copyInto(Object[] anArray) {
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            Object[] items;
            if ((items = array) != null)
                System.arraycopy(items, 0, anArray, 0, count);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /** See {@link Vector#trimToSize} */
    public void trimToSize() {
        final StampedLock lock = this.lock;
        long stamp = lock.writeLock();
        try {
            Object[] items = array;
            int n = count;
            if (items != null && n < items.length)
                array = Arrays.copyOf(items, n);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /** See {@link Vector#ensureCapacity} */
    public void ensureCapacity(int minCapacity) {
        if (minCapacity > 0) {
            final StampedLock lock = this.lock;
            long stamp = lock.writeLock();
            try {
                Object[] items = array;
                int cap = (items == null) ? 0 : items.length;
                if (minCapacity - cap > 0)
                    grow(minCapacity);
            } finally {
                lock.unlockWrite(stamp);
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
        Object[] a = null;
        int n;
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            Object[] items = array;
            if (items == null)
                n = 0;
            else {
                int len = items.length;
                if ((n = count) > len)
                    n = len;
                a = Arrays.copyOf(items, n);
            }
        } finally {
            lock.unlockRead(stamp);
        }
        return new ReadMostlyVector<E>(a, n, capacityIncrement);
    }

    private void writeObject(java.io.ObjectOutputStream s)
        throws java.io.IOException {
        final StampedLock lock = this.lock;
        long stamp = lock.readLock();
        try {
            s.defaultWriteObject();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    static final class Itr<E> implements ListIterator<E>, Enumeration<E> {
        final StampedLock lock;
        final ReadMostlyVector<E> list;
        Object[] items;
        long seq;
        int cursor;
        int fence;
        int lastRet;

        Itr(ReadMostlyVector<E> list, int index) {
            final StampedLock lock = list.lock;
            long stamp = lock.readLock();
            try {
                this.list = list;
                this.lock = lock;
                this.items = list.array;
                this.fence = list.count;
                this.cursor = index;
                this.lastRet = -1;
            } finally {
                this.seq = lock.tryConvertToOptimisticRead(stamp);
            }
            if (index < 0 || index > fence)
                throw new ArrayIndexOutOfBoundsException(index);
        }

        public boolean hasPrevious() {
            return cursor > 0;
        }

        public int nextIndex() {
            return cursor;
        }

        public int previousIndex() {
            return cursor - 1;
        }

        public boolean hasNext() {
            return cursor < fence;
        }

        public E next() {
            int i = cursor;
            Object[] es = items;
            if (es == null || i < 0 || i >= fence || i >= es.length)
                throw new NoSuchElementException();
            @SuppressWarnings("unchecked") E e = (E)es[i];
            lastRet = i;
            cursor = i + 1;
            if (!lock.validate(seq))
                throw new ConcurrentModificationException();
            return e;
        }

        public E previous() {
            int i = cursor - 1;
            Object[] es = items;
            if (es == null || i < 0 || i >= fence || i >= es.length)
                throw new NoSuchElementException();
            @SuppressWarnings("unchecked") E e = (E)es[i];
            lastRet = i;
            cursor = i;
            if (!lock.validate(seq))
                throw new ConcurrentModificationException();
            return e;
        }

        public void remove() {
            int i = lastRet;
            if (i < 0)
                throw new IllegalStateException();
            if ((seq = lock.tryConvertToWriteLock(seq)) == 0)
                throw new ConcurrentModificationException();
            try {
                list.rawRemoveAt(i);
                fence = list.count;
                cursor = i;
                lastRet = -1;
            } finally {
                seq = lock.tryConvertToOptimisticRead(seq);
            }
        }

        public void set(E e) {
            int i = lastRet;
            Object[] es = items;
            if (es == null || i < 0 | i >= fence)
                throw new IllegalStateException();
            if ((seq = lock.tryConvertToWriteLock(seq)) == 0)
                throw new ConcurrentModificationException();
            try {
                es[i] = e;
            } finally {
                seq = lock.tryConvertToOptimisticRead(seq);
            }
        }

        public void add(E e) {
            int i = cursor;
            if (i < 0)
                throw new IllegalStateException();
            if ((seq = lock.tryConvertToWriteLock(seq)) == 0)
                throw new ConcurrentModificationException();
            try {
                list.rawAddAt(i, e);
                items = list.array;
                fence = list.count;
                cursor = i + 1;
                lastRet = -1;
            } finally {
                seq = lock.tryConvertToOptimisticRead(seq);
            }
        }

        public boolean hasMoreElements() { return hasNext(); }
        public E nextElement() { return next(); }
    }

    static final class ReadMostlyVectorSublist<E>
            implements List<E>, RandomAccess, java.io.Serializable {
        private static final long serialVersionUID = 3041673470172026059L;

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
            final StampedLock lock = list.lock;
            long stamp = lock.writeLock();
            try {
                int c = size;
                list.rawAddAt(c + offset, element);
                size = c + 1;
            } finally {
                lock.unlockWrite(stamp);
            }
            return true;
        }

        public void add(int index, E element) {
            final StampedLock lock = list.lock;
            long stamp = lock.writeLock();
            try {
                if (index < 0 || index > size)
                    throw new ArrayIndexOutOfBoundsException(index);
                list.rawAddAt(index + offset, element);
                ++size;
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        public boolean addAll(Collection<? extends E> c) {
            Object[] elements = c.toArray();
            final StampedLock lock = list.lock;
            long stamp = lock.writeLock();
            try {
                int s = size;
                int pc = list.count;
                list.rawAddAllAt(offset + s, elements);
                int added = list.count - pc;
                size = s + added;
                return added != 0;
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        public boolean addAll(int index, Collection<? extends E> c) {
            Object[] elements = c.toArray();
            final StampedLock lock = list.lock;
            long stamp = lock.writeLock();
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
                lock.unlockWrite(stamp);
            }
        }

        public void clear() {
            final StampedLock lock = list.lock;
            long stamp = lock.writeLock();
            try {
                list.internalClear(offset, offset + size);
                size = 0;
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        public boolean contains(Object o) {
            return indexOf(o) >= 0;
        }

        public boolean containsAll(Collection<?> c) {
            final StampedLock lock = list.lock;
            long stamp = lock.readLock();
            try {
                return list.internalContainsAll(c, offset, offset + size);
            } finally {
                lock.unlockRead(stamp);
            }
        }

        public boolean equals(Object o) {
            if (o == this)
                return true;
            if (!(o instanceof List))
                return false;
            final StampedLock lock = list.lock;
            long stamp = lock.readLock();
            try {
                return list.internalEquals((List<?>)(o), offset, offset + size);
            } finally {
                lock.unlockRead(stamp);
            }
        }

        public E get(int index) {
            if (index < 0 || index >= size)
                throw new ArrayIndexOutOfBoundsException(index);
            return list.get(index + offset);
        }

        public int hashCode() {
            final StampedLock lock = list.lock;
            long stamp = lock.readLock();
            try {
                return list.internalHashCode(offset, offset + size);
            } finally {
                lock.unlockRead(stamp);
            }
        }

        public int indexOf(Object o) {
            final StampedLock lock = list.lock;
            long stamp = lock.readLock();
            try {
                int idx = findFirstIndex(list.array, o, offset, offset + size);
                return idx < 0 ? -1 : idx - offset;
            } finally {
                lock.unlockRead(stamp);
            }
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public Iterator<E> iterator() {
            return new SubItr<E>(this, offset);
        }

        public int lastIndexOf(Object o) {
            final StampedLock lock = list.lock;
            long stamp = lock.readLock();
            try {
                int idx = findLastIndex(list.array, o, offset + size - 1, offset);
                return idx < 0 ? -1 : idx - offset;
            } finally {
                lock.unlockRead(stamp);
            }
        }

        public ListIterator<E> listIterator() {
            return new SubItr<E>(this, offset);
        }

        public ListIterator<E> listIterator(int index) {
            return new SubItr<E>(this, index + offset);
        }

        public E remove(int index) {
            final StampedLock lock = list.lock;
            long stamp = lock.writeLock();
            try {
                Object[] items = list.array;
                int i = index + offset;
                if (items == null || index < 0 || index >= size || i >= items.length)
                    throw new ArrayIndexOutOfBoundsException(index);
                @SuppressWarnings("unchecked") E result = (E)items[i];
                list.rawRemoveAt(i);
                size--;
                return result;
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        public boolean remove(Object o) {
            final StampedLock lock = list.lock;
            long stamp = lock.writeLock();
            try {
                if (list.rawRemoveAt(findFirstIndex(list.array, o, offset,
                                                    offset + size))) {
                    --size;
                    return true;
                }
                else
                    return false;
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        public boolean removeAll(Collection<?> c) {
            return list.lockedRemoveAll(c, offset, offset + size);
        }

        public boolean retainAll(Collection<?> c) {
            return list.lockedRetainAll(c, offset, offset + size);
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
            if (fromIndex < 0)
                throw new ArrayIndexOutOfBoundsException(fromIndex);
            if (toIndex > c || ssize < 0)
                throw new ArrayIndexOutOfBoundsException(toIndex);
            return new ReadMostlyVectorSublist<E>(list, offset+fromIndex, ssize);
        }

        public Object[] toArray() {
            final StampedLock lock = list.lock;
            long stamp = lock.readLock();
            try {
                return list.internalToArray(offset, offset + size);
            } finally {
                lock.unlockRead(stamp);
            }
        }

        public <T> T[] toArray(T[] a) {
            final StampedLock lock = list.lock;
            long stamp = lock.readLock();
            try {
                return list.internalToArray(a, offset, offset + size);
            } finally {
                lock.unlockRead(stamp);
            }
        }

        public String toString() {
            final StampedLock lock = list.lock;
            long stamp = lock.readLock();
            try {
                return list.internalToString(offset, offset + size);
            } finally {
                lock.unlockRead(stamp);
            }
        }

    }

    static final class SubItr<E> implements ListIterator<E> {
        final ReadMostlyVectorSublist<E> sublist;
        final ReadMostlyVector<E> list;
        final StampedLock lock;
        Object[] items;
        long seq;
        int cursor;
        int origin;
        int fence;
        int lastRet;

        SubItr(ReadMostlyVectorSublist<E> sublist, int index) {
            final StampedLock lock = sublist.list.lock;
            long stamp = lock.readLock();
            try {
                this.sublist = sublist;
                this.list = sublist.list;
                this.lock = lock;
                this.cursor = index;
                this.origin = sublist.offset;
                this.fence = origin + sublist.size;
                this.lastRet = -1;
            } finally {
                this.seq = lock.tryConvertToOptimisticRead(stamp);
            }
            if (index < 0 || cursor > fence)
                throw new ArrayIndexOutOfBoundsException(index);
        }

        public int nextIndex() {
            return cursor - origin;
        }

        public int previousIndex() {
            return cursor - origin - 1;
        }

        public boolean hasNext() {
            return cursor < fence;
        }

        public boolean hasPrevious() {
            return cursor > origin;
        }

        public E next() {
            int i = cursor;
            Object[] es = items;
            if (es == null || i < origin || i >= fence || i >= es.length)
                throw new NoSuchElementException();
            @SuppressWarnings("unchecked") E e = (E)es[i];
            lastRet = i;
            cursor = i + 1;
            if (!lock.validate(seq))
                throw new ConcurrentModificationException();
            return e;
        }

        public E previous() {
            int i = cursor - 1;
            Object[] es = items;
            if (es == null || i < 0 || i >= fence || i >= es.length)
                throw new NoSuchElementException();
            @SuppressWarnings("unchecked") E e = (E)es[i];
            lastRet = i;
            cursor = i;
            if (!lock.validate(seq))
                throw new ConcurrentModificationException();
            return e;
        }

        public void remove() {
            int i = lastRet;
            if (i < 0)
                throw new IllegalStateException();
            if ((seq = lock.tryConvertToWriteLock(seq)) == 0)
                throw new ConcurrentModificationException();
            try {
                list.rawRemoveAt(i);
                fence = origin + sublist.size;
                cursor = i;
                lastRet = -1;
            } finally {
                seq = lock.tryConvertToOptimisticRead(seq);
            }
        }

        public void set(E e) {
            int i = lastRet;
            if (i < origin || i >= fence)
                throw new IllegalStateException();
            if ((seq = lock.tryConvertToWriteLock(seq)) == 0)
                throw new ConcurrentModificationException();
            try {
                list.set(i, e);
            } finally {
                seq = lock.tryConvertToOptimisticRead(seq);
            }
        }

        public void add(E e) {
            int i = cursor;
            if (i < origin || i >= fence)
                throw new IllegalStateException();
            if ((seq = lock.tryConvertToWriteLock(seq)) == 0)
                throw new ConcurrentModificationException();
            try {
                list.rawAddAt(i, e);
                items = list.array;
                fence = origin + sublist.size;
                cursor = i + 1;
                lastRet = -1;
            } finally {
                seq = lock.tryConvertToOptimisticRead(seq);
            }
        }
    }
}
