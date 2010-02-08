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

//////////////////////////////////////////////////
// THIS IS A GENERATED CLASS. DO NOT HAND EDIT! //
//////////////////////////////////////////////////


/**
 * Iterator for int collections.
 *
 * @author Eric D. Friedman
 * @version $Id: PIterator.template,v 1.1 2006/11/10 23:28:00 robeden Exp $
 */

public class TIntIterator extends TPrimitiveIterator {
    /**
     * the collection on which the iterator operates
     */
    private final TIntHash _hash;

    /**
     * Creates a TIntIterator for the elements in the specified collection.
     */
    public TIntIterator(TIntHash hash) {
        super(hash);
        this._hash = hash;
    }

    /**
     * Advances the iterator to the next element in the underlying collection
     * and returns it.
     *
     * @return the next int in the collection
     * @throws NoSuchElementException if the iterator is already exhausted
     */
    public int next() {
        moveToNextIndex();
        return _hash._set[_index];
    }
}// TIntIterator
