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

import java.io.Serializable;

/**
 * Interface to support pluggable hashing strategies in maps and sets.
 * Implementors can use this interface to make the trove hashing
 * algorithms use object values, values provided by the java runtime,
 * or a custom strategy when computing hashcodes.
 * <p/>
 * Created: Sat Aug 17 10:52:32 2002
 *
 * @author Eric Friedman
 * @version $Id: TObjectHashingStrategy.java,v 1.3 2007/06/11 15:26:44 robeden Exp $
 */

public interface TObjectHashingStrategy<T> extends Serializable {

    /**
     * Computes a hash code for the specified object.  Implementors
     * can use the object's own <tt>hashCode</tt> method, the Java
     * runtime's <tt>identityHashCode</tt>, or a custom scheme.
     *
     * @param object for which the hashcode is to be computed
     * @return the hashCode
     */
    int computeHashCode(T object);

    /**
     * Compares o1 and o2 for equality.  Strategy implementors may use
     * the objects' own equals() methods, compare object references,
     * or implement some custom scheme.
     *
     * @param o1 an <code>Object</code> value
     * @param o2 an <code>Object</code> value
     * @return true if the objects are equal according to this strategy.
     */
    boolean equals(T o1, T o2);
} // TObjectHashingStrategy
