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
 * This object hashing strategy uses the System.identityHashCode
 * method to provide identity hash codes.  These are identical to the
 * value produced by Object.hashCode(), even when the type of the
 * object being hashed overrides that method.
 * <p/>
 * Created: Sat Aug 17 11:13:15 2002
 *
 * @author Eric Friedman
 * @version $Id: TObjectIdentityHashingStrategy.java,v 1.4 2007/06/11 15:26:44 robeden Exp $
 */

public final class TObjectIdentityHashingStrategy<T> implements TObjectHashingStrategy<T> {
    /**
     * Delegates hash code computation to the System.identityHashCode(Object) method.
     *
     * @param object for which the hashcode is to be computed
     * @return the hashCode
     */
    public final int computeHashCode(T object) {
        return System.identityHashCode(object);
    }

    /**
     * Compares object references for equality.
     *
     * @param o1 an <code>Object</code> value
     * @param o2 an <code>Object</code> value
     * @return true if o1 == o2
     */
    public final boolean equals(T o1, T o2) {
        return o1 == o2;
    }
} // TObjectIdentityHashingStrategy
