/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package jsr166e.extra;

import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Double.longBitsToDouble;

/**
 * A {@code double} array in which elements may be updated atomically.
 * See the {@link java.util.concurrent.atomic} package specification
 * for description of the properties of atomic variables.
 *
 * <p id="bitEquals">This class compares primitive {@code double}
 * values in methods such as {@link #compareAndSet} by comparing their
 * bitwise representation using {@link Double#doubleToRawLongBits},
 * which differs from both the primitive double {@code ==} operator
 * and from {@link Double#equals}, as if implemented by:
 *  <pre> {@code
 * static boolean bitEquals(double x, double y) {
 *   long xBits = Double.doubleToRawLongBits(x);
 *   long yBits = Double.doubleToRawLongBits(y);
 *   return xBits == yBits;
 * }}</pre>
 *
 * @author Doug Lea
 * @author Martin Buchholz
 */
public class AtomicDoubleArray implements java.io.Serializable {
    private static final long serialVersionUID = -2308431214976778248L;

    private final transient long[] array;

    private long checkedByteOffset(int i) {
        if (i < 0 || i >= array.length)
            throw new IndexOutOfBoundsException("index " + i);

        return byteOffset(i);
    }

    private static long byteOffset(int i) {
        return ((long) i << shift) + base;
    }

    /**
     * Creates a new {@code AtomicDoubleArray} of the given length,
     * with all elements initially zero.
     *
     * @param length the length of the array
     */
    public AtomicDoubleArray(int length) {
        array = new long[length];
    }

    /**
     * Creates a new {@code AtomicDoubleArray} with the same length
     * as, and all elements copied from, the given array.
     *
     * @param array the array to copy elements from
     * @throws NullPointerException if array is null
     */
    public AtomicDoubleArray(double[] array) {
        // Visibility guaranteed by final field guarantees
        final int len = array.length;
        final long[] a = new long[len];
        for (int i = 0; i < len; i++)
            a[i] = doubleToRawLongBits(array[i]);
        this.array = a;
    }

    /**
     * Returns the length of the array.
     *
     * @return the length of the array
     */
    public final int length() {
        return array.length;
    }

    /**
     * Gets the current value at position {@code i}.
     *
     * @param i the index
     * @return the current value
     */
    public final double get(int i) {
        return longBitsToDouble(getRaw(checkedByteOffset(i)));
    }

    private long getRaw(long offset) {
        return unsafe.getLongVolatile(array, offset);
    }

    /**
     * Sets the element at position {@code i} to the given value.
     *
     * @param i the index
     * @param newValue the new value
     */
    public final void set(int i, double newValue) {
        long next = doubleToRawLongBits(newValue);
        unsafe.putLongVolatile(array, checkedByteOffset(i), next);
    }

    /**
     * Eventually sets the element at position {@code i} to the given value.
     *
     * @param i the index
     * @param newValue the new value
     */
    public final void lazySet(int i, double newValue) {
        long next = doubleToRawLongBits(newValue);
        unsafe.putOrderedLong(array, checkedByteOffset(i), next);
    }

    /**
     * Atomically sets the element at position {@code i} to the given value
     * and returns the old value.
     *
     * @param i the index
     * @param newValue the new value
     * @return the previous value
     */
    public final double getAndSet(int i, double newValue) {
        long next = doubleToRawLongBits(newValue);
        long offset = checkedByteOffset(i);
        while (true) {
            long current = getRaw(offset);
            if (compareAndSetRaw(offset, current, next))
                return longBitsToDouble(current);
        }
    }

    /**
     * Atomically sets the element at position {@code i} to the given
     * updated value
     * if the current value is <a href="#bitEquals">bitwise equal</a>
     * to the expected value.
     *
     * @param i the index
     * @param expect the expected value
     * @param update the new value
     * @return true if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    public final boolean compareAndSet(int i, double expect, double update) {
        return compareAndSetRaw(checkedByteOffset(i),
                                doubleToRawLongBits(expect),
                                doubleToRawLongBits(update));
    }

    private boolean compareAndSetRaw(long offset, long expect, long update) {
        return unsafe.compareAndSwapLong(array, offset, expect, update);
    }

    /**
     * Atomically sets the element at position {@code i} to the given
     * updated value
     * if the current value is <a href="#bitEquals">bitwise equal</a>
     * to the expected value.
     *
     * <p><a
     * href="http://download.oracle.com/javase/7/docs/api/java/util/concurrent/atomic/package-summary.html#Spurious">
     * May fail spuriously and does not provide ordering guarantees</a>,
     * so is only rarely an appropriate alternative to {@code compareAndSet}.
     *
     * @param i the index
     * @param expect the expected value
     * @param update the new value
     * @return true if successful
     */
    public final boolean weakCompareAndSet(int i, double expect, double update) {
        return compareAndSet(i, expect, update);
    }

    /**
     * Atomically adds the given value to the element at index {@code i}.
     *
     * @param i the index
     * @param delta the value to add
     * @return the previous value
     */
    public final double getAndAdd(int i, double delta) {
        long offset = checkedByteOffset(i);
        while (true) {
            long current = getRaw(offset);
            double currentVal = longBitsToDouble(current);
            double nextVal = currentVal + delta;
            long next = doubleToRawLongBits(nextVal);
            if (compareAndSetRaw(offset, current, next))
                return currentVal;
        }
    }

    /**
     * Atomically adds the given value to the element at index {@code i}.
     *
     * @param i the index
     * @param delta the value to add
     * @return the updated value
     */
    public double addAndGet(int i, double delta) {
        long offset = checkedByteOffset(i);
        while (true) {
            long current = getRaw(offset);
            double currentVal = longBitsToDouble(current);
            double nextVal = currentVal + delta;
            long next = doubleToRawLongBits(nextVal);
            if (compareAndSetRaw(offset, current, next))
                return nextVal;
        }
    }

    /**
     * Returns the String representation of the current values of array.
     * @return the String representation of the current values of array
     */
    public String toString() {
        int iMax = array.length - 1;
        if (iMax == -1)
            return "[]";

        // Double.toString(Math.PI).length() == 17
        StringBuilder b = new StringBuilder((17 + 2) * (iMax + 1));
        b.append('[');
        for (int i = 0;; i++) {
            b.append(longBitsToDouble(getRaw(byteOffset(i))));
            if (i == iMax)
                return b.append(']').toString();
            b.append(',').append(' ');
        }
    }

    /**
     * Saves the state to a stream (that is, serializes it).
     *
     * @param s the stream
     * @throws java.io.IOException if an I/O error occurs
     * @serialData The length of the array is emitted (int), followed by all
     *             of its elements (each a {@code double}) in the proper order.
     */
    private void writeObject(java.io.ObjectOutputStream s)
        throws java.io.IOException {
        s.defaultWriteObject();

        // Write out array length
        int length = length();
        s.writeInt(length);

        // Write out all elements in the proper order.
        for (int i = 0; i < length; i++)
            s.writeDouble(get(i));
    }

    /**
     * Reconstitutes the instance from a stream (that is, deserializes it).
     * @param s the stream
     * @throws ClassNotFoundException if the class of a serialized object
     *         could not be found
     * @throws java.io.IOException if an I/O error occurs
     */
    private void readObject(java.io.ObjectInputStream s)
        throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();

        // Read in array length and allocate array
        int length = s.readInt();
        unsafe.putObjectVolatile(this, arrayOffset, new long[length]);

        // Read in all elements in the proper order.
        for (int i = 0; i < length; i++)
            set(i, s.readDouble());
    }

    // Unsafe mechanics
    private static final sun.misc.Unsafe unsafe = getUnsafe();
    private static final long arrayOffset;
    private static final int base = unsafe.arrayBaseOffset(long[].class);
    private static final int shift;

    static {
        try {
            Class<?> k = AtomicDoubleArray.class;
            arrayOffset = unsafe.objectFieldOffset
                (k.getDeclaredField("array"));
            int scale = unsafe.arrayIndexScale(long[].class);
            if ((scale & (scale - 1)) != 0)
                throw new Error("data type scale not a power of two");
            shift = 31 - Integer.numberOfLeadingZeros(scale);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    /**
     * Returns a sun.misc.Unsafe.  Suitable for use in a 3rd party package.
     * Replace with a simple call to Unsafe.getUnsafe when integrating
     * into a jdk.
     *
     * @return a sun.misc.Unsafe
     */
    private static sun.misc.Unsafe getUnsafe() {
        try {
            return sun.misc.Unsafe.getUnsafe();
        } catch (SecurityException tryReflectionInstead) {}
        try {
            return java.security.AccessController.doPrivileged
            (new java.security.PrivilegedExceptionAction<sun.misc.Unsafe>() {
                public sun.misc.Unsafe run() throws Exception {
                    Class<sun.misc.Unsafe> k = sun.misc.Unsafe.class;
                    for (java.lang.reflect.Field f : k.getDeclaredFields()) {
                        f.setAccessible(true);
                        Object x = f.get(null);
                        if (k.isInstance(x))
                            return k.cast(x);
                    }
                    throw new NoSuchFieldError("the Unsafe");
                }});
        } catch (java.security.PrivilegedActionException e) {
            throw new RuntimeException("Could not initialize intrinsics",
                                       e.getCause());
        }
    }
}
