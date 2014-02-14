/*
 * Written by Doug Lea and Martin Buchholz with assistance from
 * members of JCP JSR-166 Expert Group and released to the public
 * domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package jsr166e.extra;

import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Double.longBitsToDouble;

/**
 * A {@code double} value that may be updated atomically.  See the
 * {@link java.util.concurrent.atomic} package specification for
 * description of the properties of atomic variables.  An {@code
 * AtomicDouble} is used in applications such as atomic accumulation,
 * and cannot be used as a replacement for a {@link Double}.  However,
 * this class does extend {@code Number} to allow uniform access by
 * tools and utilities that deal with numerically-based classes.
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
 * @see jsr166e.DoubleAdder
 * @see jsr166e.DoubleMaxUpdater
 *
 * @author Doug Lea
 * @author Martin Buchholz
 */
public class AtomicDouble extends Number implements java.io.Serializable {
    private static final long serialVersionUID = -8405198993435143622L;

    private transient volatile long value;

    /**
     * Creates a new {@code AtomicDouble} with the given initial value.
     *
     * @param initialValue the initial value
     */
    public AtomicDouble(double initialValue) {
        value = doubleToRawLongBits(initialValue);
    }

    /**
     * Creates a new {@code AtomicDouble} with initial value {@code 0.0}.
     */
    public AtomicDouble() {
        // assert doubleToRawLongBits(0.0) == 0L;
    }

    /**
     * Gets the current value.
     *
     * @return the current value
     */
    public final double get() {
        return longBitsToDouble(value);
    }

    /**
     * Sets to the given value.
     *
     * @param newValue the new value
     */
    public final void set(double newValue) {
        long next = doubleToRawLongBits(newValue);
        value = next;
    }

    /**
     * Eventually sets to the given value.
     *
     * @param newValue the new value
     */
    public final void lazySet(double newValue) {
        long next = doubleToRawLongBits(newValue);
        unsafe.putOrderedLong(this, valueOffset, next);
    }

    /**
     * Atomically sets to the given value and returns the old value.
     *
     * @param newValue the new value
     * @return the previous value
     */
    public final double getAndSet(double newValue) {
        long next = doubleToRawLongBits(newValue);
        while (true) {
            long current = value;
            if (unsafe.compareAndSwapLong(this, valueOffset, current, next))
                return longBitsToDouble(current);
        }
    }

    /**
     * Atomically sets the value to the given updated value
     * if the current value is <a href="#bitEquals">bitwise equal</a>
     * to the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return {@code true} if successful. False return indicates that
     * the actual value was not bitwise equal to the expected value.
     */
    public final boolean compareAndSet(double expect, double update) {
        return unsafe.compareAndSwapLong(this, valueOffset,
                                         doubleToRawLongBits(expect),
                                         doubleToRawLongBits(update));
    }

    /**
     * Atomically sets the value to the given updated value
     * if the current value is <a href="#bitEquals">bitwise equal</a>
     * to the expected value.
     *
     * <p><a
     * href="http://download.oracle.com/javase/7/docs/api/java/util/concurrent/atomic/package-summary.html#Spurious">
     * May fail spuriously and does not provide ordering guarantees</a>,
     * so is only rarely an appropriate alternative to {@code compareAndSet}.
     *
     * @param expect the expected value
     * @param update the new value
     * @return {@code true} if successful
     */
    public final boolean weakCompareAndSet(double expect, double update) {
        return compareAndSet(expect, update);
    }

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the previous value
     */
    public final double getAndAdd(double delta) {
        while (true) {
            long current = value;
            double currentVal = longBitsToDouble(current);
            double nextVal = currentVal + delta;
            long next = doubleToRawLongBits(nextVal);
            if (unsafe.compareAndSwapLong(this, valueOffset, current, next))
                return currentVal;
        }
    }

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     */
    public final double addAndGet(double delta) {
        while (true) {
            long current = value;
            double currentVal = longBitsToDouble(current);
            double nextVal = currentVal + delta;
            long next = doubleToRawLongBits(nextVal);
            if (unsafe.compareAndSwapLong(this, valueOffset, current, next))
                return nextVal;
        }
    }

    /**
     * Returns the String representation of the current value.
     * @return the String representation of the current value
     */
    public String toString() {
        return Double.toString(get());
    }

    /**
     * Returns the value of this {@code AtomicDouble} as an {@code int}
     * after a narrowing primitive conversion.
     */
    public int intValue() {
        return (int) get();
    }

    /**
     * Returns the value of this {@code AtomicDouble} as a {@code long}
     * after a narrowing primitive conversion.
     */
    public long longValue() {
        return (long) get();
    }

    /**
     * Returns the value of this {@code AtomicDouble} as a {@code float}
     * after a narrowing primitive conversion.
     */
    public float floatValue() {
        return (float) get();
    }

    /**
     * Returns the value of this {@code AtomicDouble} as a {@code double}.
     */
    public double doubleValue() {
        return get();
    }

    /**
     * Saves the state to a stream (that is, serializes it).
     *
     * @param s the stream
     * @throws java.io.IOException if an I/O error occurs
     * @serialData The current value is emitted (a {@code double}).
     */
    private void writeObject(java.io.ObjectOutputStream s)
        throws java.io.IOException {
        s.defaultWriteObject();

        s.writeDouble(get());
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

        set(s.readDouble());
    }

    // Unsafe mechanics
    private static final sun.misc.Unsafe unsafe = getUnsafe();
    private static final long valueOffset;

    static {
        try {
            valueOffset = unsafe.objectFieldOffset
                (AtomicDouble.class.getDeclaredField("value"));
        } catch (Exception ex) { throw new Error(ex); }
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
