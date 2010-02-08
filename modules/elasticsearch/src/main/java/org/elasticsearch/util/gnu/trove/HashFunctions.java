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
 * Provides various hash functions.
 *
 * @author wolfgang.hoschek@cern.ch
 * @version 1.0, 09/24/99
 */
public final class HashFunctions {
    /**
     * Returns a hashcode for the specified value.
     *
     * @return a hash code value for the specified value.
     */
    public static int hash(double value) {
        assert !Double.isNaN(value) : "Values of NaN are not supported.";

        long bits = Double.doubleToLongBits(value);
        return (int) (bits ^ (bits >>> 32));
        //return (int) Double.doubleToLongBits(value*663608941.737);
        //this avoids excessive hashCollisions in the case values are
        //of the form (1.0, 2.0, 3.0, ...)
    }

    /**
     * Returns a hashcode for the specified value.
     *
     * @return a hash code value for the specified value.
     */
    public static int hash(float value) {
        assert !Float.isNaN(value) : "Values of NaN are not supported.";

        return Float.floatToIntBits(value * 663608941.737f);
        // this avoids excessive hashCollisions in the case values are
        // of the form (1.0, 2.0, 3.0, ...)
    }

    /**
     * Returns a hashcode for the specified value.
     *
     * @return a hash code value for the specified value.
     */
    public static int hash(int value) {
        // Multiply by prime to make sure hash can't be negative (see Knuth v3, p. 515-516)
        return value * 31;
    }

    /**
     * Returns a hashcode for the specified value.
     *
     * @return a hash code value for the specified value.
     */
    public static int hash(long value) {
        // Multiply by prime to make sure hash can't be negative (see Knuth v3, p. 515-516)
        return ((int) (value ^ (value >>> 32))) * 31;
    }

    /**
     * Returns a hashcode for the specified object.
     *
     * @return a hash code value for the specified object.
     */
    public static int hash(Object object) {
        return object == null ? 0 : object.hashCode();
    }


    /**
     * In profiling, it has been found to be faster to have our own local implementation
     * of "ceil" rather than to call to {@link Math#ceil(double)}.
     */
    static int fastCeil(float v) {
        int possible_result = (int) v;
        if (v - possible_result > 0) possible_result++;
        return possible_result;
    }
}
