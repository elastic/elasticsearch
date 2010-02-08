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
import java.util.Random;


//////////////////////////////////////////////////
// THIS IS A GENERATED CLASS. DO NOT HAND EDIT! //
//////////////////////////////////////////////////


/**
 * A resizable, array-backed list of short primitives.
 * <p/>
 * Created: Sat Dec 29 14:21:12 2001
 *
 * @author Eric D. Friedman
 * @author Rob Eden
 */

public class TShortArrayList implements Externalizable, Cloneable {
    static final long serialVersionUID = 1L;

    /**
     * the data of the list
     */
    protected short[] _data;

    /**
     * the index after the last entry in the list
     */
    protected int _pos;

    /**
     * the default capacity for new lists
     */
    protected static final int DEFAULT_CAPACITY = 10;

    /**
     * Creates a new <code>TShortArrayList</code> instance with the
     * default capacity.
     */
    public TShortArrayList() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Creates a new <code>TShortArrayList</code> instance with the
     * specified capacity.
     *
     * @param capacity an <code>int</code> value
     */
    public TShortArrayList(int capacity) {
        _data = new short[capacity];
        _pos = 0;
    }

    /**
     * Creates a new <code>TShortArrayList</code> instance whose
     * capacity is the greater of the length of <tt>values</tt> and
     * DEFAULT_CAPACITY and whose initial contents are the specified
     * values.
     *
     * @param values an <code>short[]</code> value
     */
    public TShortArrayList(short[] values) {
        this(Math.max(values.length, DEFAULT_CAPACITY));
        add(values);
    }

    // sizing

    /**
     * Grow the internal array as needed to accommodate the specified
     * number of elements.  The size of the array shorts on each
     * resize unless <tt>capacity</tt> requires more than twice the
     * current capacity.
     *
     * @param capacity an <code>int</code> value
     */
    public void ensureCapacity(int capacity) {
        if (capacity > _data.length) {
            int newCap = Math.max(_data.length << 1, capacity);
            short[] tmp = new short[newCap];
            System.arraycopy(_data, 0, tmp, 0, _data.length);
            _data = tmp;
        }
    }

    /**
     * Returns the number of values in the list.
     *
     * @return the number of values in the list.
     */
    public int size() {
        return _pos;
    }

    /**
     * Tests whether this list contains any values.
     *
     * @return true if the list is empty.
     */
    public boolean isEmpty() {
        return _pos == 0;
    }

    /**
     * Sheds any excess capacity above and beyond the current size of
     * the list.
     */
    public void trimToSize() {
        if (_data.length > size()) {
            short[] tmp = new short[size()];
            toNativeArray(tmp, 0, tmp.length);
            _data = tmp;
        }
    }

    // modifying

    /**
     * Adds <tt>val</tt> to the end of the list, growing as needed.
     *
     * @param val an <code>short</code> value
     */
    public void add(short val) {
        ensureCapacity(_pos + 1);
        _data[_pos++] = val;
    }

    /**
     * Adds the values in the array <tt>vals</tt> to the end of the
     * list, in order.
     *
     * @param vals an <code>short[]</code> value
     */
    public void add(short[] vals) {
        add(vals, 0, vals.length);
    }

    /**
     * Adds a subset of the values in the array <tt>vals</tt> to the
     * end of the list, in order.
     *
     * @param vals   an <code>short[]</code> value
     * @param offset the offset at which to start copying
     * @param length the number of values to copy.
     */
    public void add(short[] vals, int offset, int length) {
        ensureCapacity(_pos + length);
        System.arraycopy(vals, offset, _data, _pos, length);
        _pos += length;
    }

    /**
     * Inserts <tt>value</tt> into the list at <tt>offset</tt>.  All
     * values including and to the right of <tt>offset</tt> are shifted
     * to the right.
     *
     * @param offset an <code>int</code> value
     * @param value  an <code>short</code> value
     */
    public void insert(int offset, short value) {
        if (offset == _pos) {
            add(value);
            return;
        }
        ensureCapacity(_pos + 1);
        // shift right
        System.arraycopy(_data, offset, _data, offset + 1, _pos - offset);
        // insert
        _data[offset] = value;
        _pos++;
    }

    /**
     * Inserts the array of <tt>values</tt> into the list at
     * <tt>offset</tt>.  All values including and to the right of
     * <tt>offset</tt> are shifted to the right.
     *
     * @param offset an <code>int</code> value
     * @param values an <code>short[]</code> value
     */
    public void insert(int offset, short[] values) {
        insert(offset, values, 0, values.length);
    }

    /**
     * Inserts a slice of the array of <tt>values</tt> into the list
     * at <tt>offset</tt>.  All values including and to the right of
     * <tt>offset</tt> are shifted to the right.
     *
     * @param offset    an <code>int</code> value
     * @param values    an <code>short[]</code> value
     * @param valOffset the offset in the values array at which to
     *                  start copying.
     * @param len       the number of values to copy from the values array
     */
    public void insert(int offset, short[] values, int valOffset, int len) {
        if (offset == _pos) {
            add(values, valOffset, len);
            return;
        }

        ensureCapacity(_pos + len);
        // shift right
        System.arraycopy(_data, offset, _data, offset + len, _pos - offset);
        // insert
        System.arraycopy(values, valOffset, _data, offset, len);
        _pos += len;
    }

    /**
     * Returns the value at the specified offset.
     *
     * @param offset an <code>int</code> value
     * @return an <code>short</code> value
     */
    public short get(int offset) {
        if (offset >= _pos) {
            throw new ArrayIndexOutOfBoundsException(offset);
        }
        return _data[offset];
    }

    /**
     * Returns the value at the specified offset without doing any
     * bounds checking.
     *
     * @param offset an <code>int</code> value
     * @return an <code>short</code> value
     */
    public short getQuick(int offset) {
        return _data[offset];
    }

    /**
     * Sets the value at the specified offset.
     *
     * @param offset an <code>int</code> value
     * @param val    an <code>short</code> value
     */
    public void set(int offset, short val) {
        if (offset >= _pos) {
            throw new ArrayIndexOutOfBoundsException(offset);
        }
        _data[offset] = val;
    }

    /**
     * Sets the value at the specified offset and returns the
     * previously stored value.
     *
     * @param offset an <code>int</code> value
     * @param val    an <code>short</code> value
     * @return the value previously stored at offset.
     */
    public short getSet(int offset, short val) {
        if (offset >= _pos) {
            throw new ArrayIndexOutOfBoundsException(offset);
        }
        short old = _data[offset];
        _data[offset] = val;
        return old;
    }

    /**
     * Replace the values in the list starting at <tt>offset</tt> with
     * the contents of the <tt>values</tt> array.
     *
     * @param offset the first offset to replace
     * @param values the source of the new values
     */
    public void set(int offset, short[] values) {
        set(offset, values, 0, values.length);
    }

    /**
     * Replace the values in the list starting at <tt>offset</tt> with
     * <tt>length</tt> values from the <tt>values</tt> array, starting
     * at valOffset.
     *
     * @param offset    the first offset to replace
     * @param values    the source of the new values
     * @param valOffset the first value to copy from the values array
     * @param length    the number of values to copy
     */
    public void set(int offset, short[] values, int valOffset, int length) {
        if (offset < 0 || offset + length > _pos) {
            throw new ArrayIndexOutOfBoundsException(offset);
        }
        System.arraycopy(values, valOffset, _data, offset, length);
    }

    /**
     * Sets the value at the specified offset without doing any bounds
     * checking.
     *
     * @param offset an <code>int</code> value
     * @param val    an <code>short</code> value
     */
    public void setQuick(int offset, short val) {
        _data[offset] = val;
    }

    /**
     * Flushes the internal state of the list, resetting the capacity
     * to the default.
     */
    public void clear() {
        clear(DEFAULT_CAPACITY);
    }

    /**
     * Flushes the internal state of the list, setting the capacity of
     * the empty list to <tt>capacity</tt>.
     *
     * @param capacity an <code>int</code> value
     */
    public void clear(int capacity) {
        _data = new short[capacity];
        _pos = 0;
    }

    /**
     * Sets the size of the list to 0, but does not change its
     * capacity.  This method can be used as an alternative to the
     * {@link #clear clear} method if you want to recyle a list without
     * allocating new backing arrays.
     *
     * @see #clear
     */
    public void reset() {
        _pos = 0;
        fill((short) 0);
    }

    /**
     * Sets the size of the list to 0, but does not change its
     * capacity.  This method can be used as an alternative to the
     * {@link #clear clear} method if you want to recyle a list
     * without allocating new backing arrays.  This method differs
     * from {@link #reset reset} in that it does not clear the old
     * values in the backing array.  Thus, it is possible for {@link
     * #getQuick getQuick} to return stale data if this method is used
     * and the caller is careless about bounds checking.
     *
     * @see #reset
     * @see #clear
     * @see #getQuick
     */
    public void resetQuick() {
        _pos = 0;
    }

    /**
     * Removes the value at <tt>offset</tt> from the list.
     *
     * @param offset an <code>int</code> value
     * @return the value previously stored at offset.
     */
    public short remove(int offset) {
        short old = get(offset);
        remove(offset, 1);
        return old;
    }

    /**
     * Removes <tt>length</tt> values from the list, starting at
     * <tt>offset</tt>
     *
     * @param offset an <code>int</code> value
     * @param length an <code>int</code> value
     */
    public void remove(int offset, int length) {
        if (offset < 0 || offset >= _pos) {
            throw new ArrayIndexOutOfBoundsException(offset);
        }

        if (offset == 0) {
            // data at the front
            System.arraycopy(_data, length, _data, 0, _pos - length);
        } else if (_pos - length == offset) {
            // no copy to make, decrementing pos "deletes" values at
            // the end
        } else {
            // data in the middle
            System.arraycopy(_data, offset + length,
                    _data, offset, _pos - (offset + length));
        }
        _pos -= length;
        // no need to clear old values beyond _pos, because this is a
        // primitive collection and 0 takes as much room as any other
        // value
    }

    /**
     * Transform each value in the list using the specified function.
     *
     * @param function a <code>TShortFunction</code> value
     */
    public void transformValues(TShortFunction function) {
        for (int i = _pos; i-- > 0;) {
            _data[i] = function.execute(_data[i]);
        }
    }

    /**
     * Reverse the order of the elements in the list.
     */
    public void reverse() {
        reverse(0, _pos);
    }

    /**
     * Reverse the order of the elements in the range of the list.
     *
     * @param from the inclusive index at which to start reversing
     * @param to   the exclusive index at which to stop reversing
     */
    public void reverse(int from, int to) {
        if (from == to) {
            return;             // nothing to do
        }
        if (from > to) {
            throw new IllegalArgumentException("from cannot be greater than to");
        }
        for (int i = from, j = to - 1; i < j; i++, j--) {
            swap(i, j);
        }
    }

    /**
     * Shuffle the elements of the list using the specified random
     * number generator.
     *
     * @param rand a <code>Random</code> value
     */
    public void shuffle(Random rand) {
        for (int i = _pos; i-- > 1;) {
            swap(i, rand.nextInt(i));
        }
    }

    /**
     * Swap the values at offsets <tt>i</tt> and <tt>j</tt>.
     *
     * @param i an offset into the data array
     * @param j an offset into the data array
     */
    private final void swap(int i, int j) {
        short tmp = _data[i];
        _data[i] = _data[j];
        _data[j] = tmp;
    }

    // copying

    /**
     * Returns a clone of this list.  Since this is a primitive
     * collection, this will be a deep clone.
     *
     * @return a deep clone of the list.
     */
    public Object clone() {
        TShortArrayList list = null;
        try {
            list = (TShortArrayList) super.clone();
            list._data = toNativeArray();
        } catch (CloneNotSupportedException e) {
            // it's supported
        } // end of try-catch
        return list;
    }


    /**
     * Returns a sublist of this list.
     *
     * @param begin low endpoint (inclusive) of the subList.
     * @param end   high endpoint (exclusive) of the subList.
     * @return sublist of this list from begin, inclusive to end, exclusive.
     * @throws IndexOutOfBoundsException - endpoint out of range
     * @throws IllegalArgumentException  - endpoints out of order (end > begin)
     */
    public TShortArrayList subList(int begin, int end) {
        if (end < begin) throw new IllegalArgumentException("end index " + end + " greater than begin index " + begin);
        if (begin < 0) throw new IndexOutOfBoundsException("begin index can not be < 0");
        if (end > _data.length) throw new IndexOutOfBoundsException("end index < " + _data.length);
        TShortArrayList list = new TShortArrayList(end - begin);
        for (int i = begin; i < end; i++) {
            list.add(_data[i]);
        }
        return list;
    }


    /**
     * Copies the contents of the list into a native array.
     *
     * @return an <code>short[]</code> value
     */
    public short[] toNativeArray() {
        return toNativeArray(0, _pos);
    }

    /**
     * Copies a slice of the list into a native array.
     *
     * @param offset the offset at which to start copying
     * @param len    the number of values to copy.
     * @return an <code>short[]</code> value
     */
    public short[] toNativeArray(int offset, int len) {
        short[] rv = new short[len];
        toNativeArray(rv, offset, len);
        return rv;
    }

    /**
     * Copies a slice of the list into a native array.
     *
     * @param dest   the array to copy into.
     * @param offset the offset of the first value to copy
     * @param len    the number of values to copy.
     */
    public void toNativeArray(short[] dest, int offset, int len) {
        if (len == 0) {
            return;             // nothing to copy
        }
        if (offset < 0 || offset >= _pos) {
            throw new ArrayIndexOutOfBoundsException(offset);
        }
        System.arraycopy(_data, offset, dest, 0, len);
    }

    // comparing

    /**
     * Compares this list to another list, value by value.
     *
     * @param other the object to compare against
     * @return true if other is a TShortArrayList and has exactly the
     *         same values.
     */
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other instanceof TShortArrayList) {
            TShortArrayList that = (TShortArrayList) other;
            if (that.size() != this.size()) {
                return false;
            } else {
                for (int i = _pos; i-- > 0;) {
                    if (this._data[i] != that._data[i]) {
                        return false;
                    }
                }
                return true;
            }
        } else {
            return false;
        }
    }

    public int hashCode() {
        int h = 0;
        for (int i = _pos; i-- > 0;) {
            h = 37 * h + HashFunctions.hash(_data[i]);
        }
        return h;
    }

    // procedures

    /**
     * Applies the procedure to each value in the list in ascending
     * (front to back) order.
     *
     * @param procedure a <code>TShortProcedure</code> value
     * @return true if the procedure did not terminate prematurely.
     */
    public boolean forEach(TShortProcedure procedure) {
        for (int i = 0; i < _pos; i++) {
            if (!procedure.execute(_data[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Applies the procedure to each value in the list in descending
     * (back to front) order.
     *
     * @param procedure a <code>TShortProcedure</code> value
     * @return true if the procedure did not terminate prematurely.
     */
    public boolean forEachDescending(TShortProcedure procedure) {
        for (int i = _pos; i-- > 0;) {
            if (!procedure.execute(_data[i])) {
                return false;
            }
        }
        return true;
    }

    // sorting

    /**
     * Sort the values in the list (ascending) using the Sun quicksort
     * implementation.
     *
     * @see java.util.Arrays#sort
     */
    public void sort() {
        Arrays.sort(_data, 0, _pos);
    }

    /**
     * Sort a slice of the list (ascending) using the Sun quicksort
     * implementation.
     *
     * @param fromIndex the index at which to start sorting (inclusive)
     * @param toIndex   the index at which to stop sorting (exclusive)
     * @see java.util.Arrays#sort
     */
    public void sort(int fromIndex, int toIndex) {
        Arrays.sort(_data, fromIndex, toIndex);
    }

    // filling

    /**
     * Fills every slot in the list with the specified value.
     *
     * @param val the value to use when filling
     */
    public void fill(short val) {
        Arrays.fill(_data, 0, _pos, val);
    }

    /**
     * Fills a range in the list with the specified value.
     *
     * @param fromIndex the offset at which to start filling (inclusive)
     * @param toIndex   the offset at which to stop filling (exclusive)
     * @param val       the value to use when filling
     */
    public void fill(int fromIndex, int toIndex, short val) {
        if (toIndex > _pos) {
            ensureCapacity(toIndex);
            _pos = toIndex;
        }
        Arrays.fill(_data, fromIndex, toIndex, val);
    }

    // searching

    /**
     * Performs a binary search for <tt>value</tt> in the entire list.
     * Note that you <b>must</b> @{link #sort sort} the list before
     * doing a search.
     *
     * @param value the value to search for
     * @return the absolute offset in the list of the value, or its
     *         negative insertion point into the sorted list.
     */
    public int binarySearch(short value) {
        return binarySearch(value, 0, _pos);
    }

    /**
     * Performs a binary search for <tt>value</tt> in the specified
     * range.  Note that you <b>must</b> @{link #sort sort} the list
     * or the range before doing a search.
     *
     * @param value     the value to search for
     * @param fromIndex the lower boundary of the range (inclusive)
     * @param toIndex   the upper boundary of the range (exclusive)
     * @return the absolute offset in the list of the value, or its
     *         negative insertion point into the sorted list.
     */
    public int binarySearch(short value, int fromIndex, int toIndex) {
        if (fromIndex < 0) {
            throw new ArrayIndexOutOfBoundsException(fromIndex);
        }
        if (toIndex > _pos) {
            throw new ArrayIndexOutOfBoundsException(toIndex);
        }

        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            short midVal = _data[mid];

            if (midVal < value) {
                low = mid + 1;
            } else if (midVal > value) {
                high = mid - 1;
            } else {
                return mid; // value found
            }
        }
        return -(low + 1);  // value not found.
    }

    /**
     * Searches the list front to back for the index of
     * <tt>value</tt>.
     *
     * @param value an <code>short</code> value
     * @return the first offset of the value, or -1 if it is not in
     *         the list.
     * @see #binarySearch for faster searches on sorted lists
     */
    public int indexOf(short value) {
        return indexOf(0, value);
    }

    /**
     * Searches the list front to back for the index of
     * <tt>value</tt>, starting at <tt>offset</tt>.
     *
     * @param offset the offset at which to start the linear search
     *               (inclusive)
     * @param value  an <code>short</code> value
     * @return the first offset of the value, or -1 if it is not in
     *         the list.
     * @see #binarySearch for faster searches on sorted lists
     */
    public int indexOf(int offset, short value) {
        for (int i = offset; i < _pos; i++) {
            if (_data[i] == value) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Searches the list back to front for the last index of
     * <tt>value</tt>.
     *
     * @param value an <code>short</code> value
     * @return the last offset of the value, or -1 if it is not in
     *         the list.
     * @see #binarySearch for faster searches on sorted lists
     */
    public int lastIndexOf(short value) {
        return lastIndexOf(_pos, value);
    }

    /**
     * Searches the list back to front for the last index of
     * <tt>value</tt>, starting at <tt>offset</tt>.
     *
     * @param offset the offset at which to start the linear search
     *               (exclusive)
     * @param value  an <code>short</code> value
     * @return the last offset of the value, or -1 if it is not in
     *         the list.
     * @see #binarySearch for faster searches on sorted lists
     */
    public int lastIndexOf(int offset, short value) {
        for (int i = offset; i-- > 0;) {
            if (_data[i] == value) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Searches the list for <tt>value</tt>
     *
     * @param value an <code>short</code> value
     * @return true if value is in the list.
     */
    public boolean contains(short value) {
        return lastIndexOf(value) >= 0;
    }

    /**
     * Searches the list for values satisfying <tt>condition</tt> in
     * the manner of the *nix <tt>grep</tt> utility.
     *
     * @param condition a condition to apply to each element in the list
     * @return a list of values which match the condition.
     */
    public TShortArrayList grep(TShortProcedure condition) {
        TShortArrayList list = new TShortArrayList();
        for (int i = 0; i < _pos; i++) {
            if (condition.execute(_data[i])) {
                list.add(_data[i]);
            }
        }
        return list;
    }

    /**
     * Searches the list for values which do <b>not</b> satisfy
     * <tt>condition</tt>.  This is akin to *nix <code>grep -v</code>.
     *
     * @param condition a condition to apply to each element in the list
     * @return a list of values which do not match the condition.
     */
    public TShortArrayList inverseGrep(TShortProcedure condition) {
        TShortArrayList list = new TShortArrayList();
        for (int i = 0; i < _pos; i++) {
            if (!condition.execute(_data[i])) {
                list.add(_data[i]);
            }
        }
        return list;
    }

    /**
     * Finds the maximum value in the list.
     *
     * @return the largest value in the list.
     * @throws IllegalStateException if the list is empty
     */
    public short max() {
        if (size() == 0) {
            throw new IllegalStateException("cannot find maximum of an empty list");
        }
        short max = Short.MIN_VALUE;
        for (int i = 0; i < _pos; i++) {
            if (_data[i] > max) {
                max = _data[i];
            }
        }
        return max;
    }

    /**
     * Finds the minimum value in the list.
     *
     * @return the smallest value in the list.
     * @throws IllegalStateException if the list is empty
     */
    public short min() {
        if (size() == 0) {
            throw new IllegalStateException("cannot find minimum of an empty list");
        }
        short min = Short.MAX_VALUE;
        for (int i = 0; i < _pos; i++) {
            if (_data[i] < min) {
                min = _data[i];
            }
        }
        return min;
    }

    // stringification

    /**
     * Returns a String representation of the list, front to back.
     *
     * @return a <code>String</code> value
     */
    public String toString() {
        final StringBuilder buf = new StringBuilder("{");
        for (int i = 0, end = _pos - 1; i < end; i++) {
            buf.append(_data[i]);
            buf.append(", ");
        }
        if (size() > 0) {
            buf.append(_data[_pos - 1]);
        }
        buf.append("}");
        return buf.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        // VERSION
        out.writeByte(1);

        // POSITION
        out.writeInt(_pos);

        // ENTRIES
        int len = _pos;
        out.writeInt(_pos);    // Written twice for backwards compatability with
        // version 0
        for (int i = 0; i < len; i++) {
            out.writeShort(_data[i]);
        }
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {

        // VERSION
        in.readByte();

        // POSITION
        _pos = in.readInt();

        // ENTRIES
        int len = in.readInt();
        _data = new short[len];
        for (int i = 0; i < len; i++) {
            _data[i] = in.readShort();
        }
    }
} // TShortArrayList
