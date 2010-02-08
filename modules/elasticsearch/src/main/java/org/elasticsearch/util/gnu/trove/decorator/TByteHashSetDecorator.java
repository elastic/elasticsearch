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

package org.elasticsearch.util.gnu.trove.decorator;

import org.elasticsearch.util.gnu.trove.TByteHashSet;
import org.elasticsearch.util.gnu.trove.TByteIterator;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;


//////////////////////////////////////////////////
// THIS IS A GENERATED CLASS. DO NOT HAND EDIT! //
//////////////////////////////////////////////////


/**
 * Wrapper class to make a TByteHashSet conform to the <tt>java.util.Set</tt> API.
 * This class simply decorates an underlying TByteHashSet and translates the Object-based
 * APIs into their Trove primitive analogs.
 * <p/>
 * <p/>
 * Note that wrapping and unwrapping primitive values is extremely inefficient.  If
 * possible, users of this class should override the appropriate methods in this class
 * and use a table of canonical values.
 * </p>
 * <p/>
 * Created: Tue Sep 24 22:08:17 PDT 2002
 *
 * @author Eric D. Friedman
 */
public class TByteHashSetDecorator extends AbstractSet<Byte>
        implements Set<Byte>, Externalizable {

    /**
     * the wrapped primitive set
     */
    protected TByteHashSet _set;


    /**
     * FOR EXTERNALIZATION ONLY!!
     */
    public TByteHashSetDecorator() {
    }

    /**
     * Creates a wrapper that decorates the specified primitive set.
     */
    public TByteHashSetDecorator(TByteHashSet set) {
        super();
        this._set = set;
    }


    /**
     * Returns a reference to the set wrapped by this decorator.
     */
    public TByteHashSet getSet() {
        return _set;
    }

    /**
     * Clones the underlying trove collection and returns the clone wrapped in a new
     * decorator instance.  This is a shallow clone except where primitives are
     * concerned.
     *
     * @return a copy of the receiver
     */
    public TByteHashSetDecorator clone() {
        try {
            TByteHashSetDecorator copy = (TByteHashSetDecorator) super.clone();
            copy._set = (TByteHashSet) _set.clone();
            return copy;
        } catch (CloneNotSupportedException e) {
            // assert(false);
            throw new InternalError(); // we are cloneable
        }
    }

    /**
     * Inserts a value into the set.
     *
     * @param value true if the set was modified by the insertion
     */
    public boolean add(Byte value) {
        return _set.add(unwrap(value));
    }

    /**
     * Compares this set with another set for equality of their stored
     * entries.
     *
     * @param other an <code>Object</code> value
     * @return true if the sets are identical
     */
    public boolean equals(Object other) {
        if (_set.equals(other)) {
            return true;    // comparing two trove sets
        } else if (other instanceof Set) {
            Set that = (Set) other;
            if (that.size() != _set.size()) {
                return false;    // different sizes, no need to compare
            } else {        // now we have to do it the hard way
                Iterator it = that.iterator();
                for (int i = that.size(); i-- > 0;) {
                    Object val = it.next();
                    if (val instanceof Byte) {
                        byte v = unwrap(val);
                        if (_set.contains(v)) {
                            // match, ok to continue
                        } else {
                            return false; // no match: we're done
                        }
                    } else {
                        return false; // different type in other set
                    }
                }
                return true;    // all entries match
            }
        } else {
            return false;
        }
    }

    /**
     * Empties the set.
     */
    public void clear() {
        this._set.clear();
    }

    /**
     * Deletes a value from the set.
     *
     * @param value an <code>Object</code> value
     * @return true if the set was modified
     */
    public boolean remove(Object value) {
        return _set.remove(unwrap(value));
    }

    /**
     * Creates an iterator over the values of the set.
     *
     * @return an iterator with support for removals in the underlying set
     */
    public Iterator<Byte> iterator() {
        return new Iterator<Byte>() {
            private final TByteIterator it = _set.iterator();

            public Byte next() {
                return wrap(it.next());
            }

            public boolean hasNext() {
                return it.hasNext();
            }

            public void remove() {
                it.remove();
            }
        };
    }

    /**
     * Returns the number of entries in the set.
     *
     * @return the set's size.
     */
    public int size() {
        return this._set.size();
    }

    /**
     * Indicates whether set has any entries.
     *
     * @return true if the set is empty
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Wraps a value
     *
     * @param k value in the underlying set
     * @return an Object representation of the value
     */
    protected Byte wrap(byte k) {
        return Byte.valueOf(k);
    }

    /**
     * Unwraps a value
     *
     * @param value wrapped value
     * @return an unwrapped representation of the value
     */
    protected byte unwrap(Object value) {
        return ((Byte) value).byteValue();
    }


    // Implements Externalizable

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {

        // VERSION
        in.readByte();

        // SET
        _set = (TByteHashSet) in.readObject();
    }


    // Implements Externalizable

    public void writeExternal(ObjectOutput out) throws IOException {
        // VERSION
        out.writeByte(0);

        // SET
        out.writeObject(_set);
    }
} // TByteHashSetDecorator
