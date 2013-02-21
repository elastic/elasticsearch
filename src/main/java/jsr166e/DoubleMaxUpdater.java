/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package jsr166e;
import java.io.Serializable;

/**
 * One or more variables that together maintain a running {@code double}
 * maximum with initial value {@code Double.NEGATIVE_INFINITY}.  When
 * updates (method {@link #update}) are contended across threads, the
 * set of variables may grow dynamically to reduce contention.  Method
 * {@link #max} (or, equivalently, {@link #doubleValue}) returns the
 * current maximum across the variables maintaining updates.
 *
 * <p>This class extends {@link Number}, but does <em>not</em> define
 * methods such as {@code equals}, {@code hashCode} and {@code
 * compareTo} because instances are expected to be mutated, and so are
 * not useful as collection keys.
 *
 * <p><em>jsr166e note: This class is targeted to be placed in
 * java.util.concurrent.atomic.</em>
 *
 * @since 1.8
 * @author Doug Lea
 */
public class DoubleMaxUpdater extends Striped64 implements Serializable {
    private static final long serialVersionUID = 7249069246863182397L;
    /**
     * Long representation of negative infinity. See class Double
     * internal documentation for explanation.
     */
    private static final long MIN_AS_LONG = 0xfff0000000000000L;

    /**
     * Update function. See class DoubleAdder for rationale
     * for using conversions from/to long.
     */
    final long fn(long v, long x) {
        return Double.longBitsToDouble(v) > Double.longBitsToDouble(x) ? v : x;
    }

    /**
     * Creates a new instance with initial value of {@code
     * Double.NEGATIVE_INFINITY}.
     */
    public DoubleMaxUpdater() {
        base = MIN_AS_LONG;
    }

    /**
     * Updates the maximum to be at least the given value.
     *
     * @param x the value to update
     */
    public void update(double x) {
        long lx = Double.doubleToRawLongBits(x);
        Cell[] as; long b, v; HashCode hc; Cell a; int n;
        if ((as = cells) != null ||
            (Double.longBitsToDouble(b = base) < x && !casBase(b, lx))) {
            boolean uncontended = true;
            int h = (hc = threadHashCode.get()).code;
            if (as == null || (n = as.length) < 1 ||
                (a = as[(n - 1) & h]) == null ||
                (Double.longBitsToDouble(v = a.value) < x &&
                 !(uncontended = a.cas(v, lx))))
                retryUpdate(lx, hc, uncontended);
        }
    }

    /**
     * Returns the current maximum.  The returned value is
     * <em>NOT</em> an atomic snapshot; invocation in the absence of
     * concurrent updates returns an accurate result, but concurrent
     * updates that occur while the value is being calculated might
     * not be incorporated.
     *
     * @return the maximum
     */
    public double max() {
        Cell[] as = cells;
        double max = Double.longBitsToDouble(base);
        if (as != null) {
            int n = as.length;
            double v;
            for (int i = 0; i < n; ++i) {
                Cell a = as[i];
                if (a != null && (v = Double.longBitsToDouble(a.value)) > max)
                    max = v;
            }
        }
        return max;
    }

    /**
     * Resets variables maintaining updates to {@code
     * Double.NEGATIVE_INFINITY}.  This method may be a useful
     * alternative to creating a new updater, but is only effective if
     * there are no concurrent updates.  Because this method is
     * intrinsically racy, it should only be used when it is known
     * that no threads are concurrently updating.
     */
    public void reset() {
        internalReset(MIN_AS_LONG);
    }

    /**
     * Equivalent in effect to {@link #max} followed by {@link
     * #reset}. This method may apply for example during quiescent
     * points between multithreaded computations.  If there are
     * updates concurrent with this method, the returned value is
     * <em>not</em> guaranteed to be the final value occurring before
     * the reset.
     *
     * @return the maximum
     */
    public double maxThenReset() {
        Cell[] as = cells;
        double max = Double.longBitsToDouble(base);
        base = MIN_AS_LONG;
        if (as != null) {
            int n = as.length;
            for (int i = 0; i < n; ++i) {
                Cell a = as[i];
                if (a != null) {
                    double v = Double.longBitsToDouble(a.value);
                    a.value = MIN_AS_LONG;
                    if (v > max)
                        max = v;
                }
            }
        }
        return max;
    }

    /**
     * Returns the String representation of the {@link #max}.
     * @return the String representation of the {@link #max}
     */
    public String toString() {
        return Double.toString(max());
    }

    /**
     * Equivalent to {@link #max}.
     *
     * @return the max
     */
    public double doubleValue() {
        return max();
    }

    /**
     * Returns the {@link #max} as a {@code long} after a
     * narrowing primitive conversion.
     */
    public long longValue() {
        return (long)max();
    }

    /**
     * Returns the {@link #max} as an {@code int} after a
     * narrowing primitive conversion.
     */
    public int intValue() {
        return (int)max();
    }

    /**
     * Returns the {@link #max} as a {@code float}
     * after a narrowing primitive conversion.
     */
    public float floatValue() {
        return (float)max();
    }

    private void writeObject(java.io.ObjectOutputStream s)
        throws java.io.IOException {
        s.defaultWriteObject();
        s.writeDouble(max());
    }

    private void readObject(java.io.ObjectInputStream s)
        throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();
        busy = 0;
        cells = null;
        base = Double.doubleToRawLongBits(s.readDouble());
    }

}
