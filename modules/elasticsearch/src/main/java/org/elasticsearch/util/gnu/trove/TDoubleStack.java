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

/**
 * A stack of double primitives, backed by a TDoubleArrayList.
 *
 * @author Eric D. Friedman, Rob Eden
 * @version $Id: PStack.template,v 1.2 2007/02/28 23:03:57 robeden Exp $
 */

public class TDoubleStack {

    /**
     * the list used to hold the stack values.
     */
    protected TDoubleArrayList _list;

    public static final int DEFAULT_CAPACITY = TDoubleArrayList.DEFAULT_CAPACITY;

    /**
     * Creates a new <code>TDoubleStack</code> instance with the default
     * capacity.
     */
    public TDoubleStack() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Creates a new <code>TDoubleStack</code> instance with the
     * specified capacity.
     *
     * @param capacity the initial depth of the stack
     */
    public TDoubleStack(int capacity) {
        _list = new TDoubleArrayList(capacity);
    }

    /**
     * Pushes the value onto the top of the stack.
     *
     * @param val an <code>double</code> value
     */
    public void push(double val) {
        _list.add(val);
    }

    /**
     * Removes and returns the value at the top of the stack.
     *
     * @return an <code>double</code> value
     */
    public double pop() {
        return _list.remove(_list.size() - 1);
    }

    /**
     * Returns the value at the top of the stack.
     *
     * @return an <code>double</code> value
     */
    public double peek() {
        return _list.get(_list.size() - 1);
    }

    /**
     * Returns the current depth of the stack.
     */
    public int size() {
        return _list.size();
    }

    /**
     * Clears the stack, reseting its capacity to the default.
     */
    public void clear() {
        _list.clear(DEFAULT_CAPACITY);
    }

    /**
     * Clears the stack without releasing its internal capacity allocation.
     */
    public void reset() {
        _list.reset();
    }

    /**
     * Copies the contents of the stack into a native array. Note that this will NOT
     * pop them out of the stack.
     *
     * @return an <code>double[]</code> value
     */
    public double[] toNativeArray() {
        return _list.toNativeArray();
    }

    /**
     * Copies a slice of the list into a native array. Note that this will NOT
     * pop them out of the stack.
     *
     * @param dest the array to copy into.
     */
    public void toNativeArray(double[] dest) {
        _list.toNativeArray(dest, 0, size());
    }
} // TDoubleStack
