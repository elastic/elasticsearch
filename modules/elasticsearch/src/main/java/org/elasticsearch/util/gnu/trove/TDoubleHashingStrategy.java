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

//////////////////////////////////////////////////
// THIS IS A GENERATED CLASS. DO NOT HAND EDIT! //
//////////////////////////////////////////////////


/**
 * Interface to support pluggable hashing strategies in maps and sets.
 * Implementors can use this interface to make the trove hashing
 * algorithms use an optimal strategy when computing hashcodes.
 * <p/>
 * Created: Sun Nov  4 08:56:06 2001
 *
 * @author Eric D. Friedman
 * @version $Id: PHashingStrategy.template,v 1.1 2006/11/10 23:28:00 robeden Exp $
 */

public interface TDoubleHashingStrategy extends Serializable {
    /**
     * Computes a hash code for the specified double.  Implementors
     * can use the double's own value or a custom scheme designed to
     * minimize collisions for a known set of input.
     *
     * @param val double for which the hashcode is to be computed
     * @return the hashCode
     */
    public int computeHashCode(double val);
} // TDoubleHashingStrategy
