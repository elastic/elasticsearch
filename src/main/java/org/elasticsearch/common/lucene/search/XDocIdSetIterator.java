/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.common.lucene.docset.DocIdSets;

/**
 * Extension of {@link DocIdSetIterator} that allows to know if iteration is
 * implemented efficiently.
 */
public abstract class XDocIdSetIterator extends DocIdSetIterator {

    /**
     * Return <tt>true</tt> if this iterator cannot both
     * {@link DocIdSetIterator#nextDoc} and {@link DocIdSetIterator#advance}
     * in sub-linear time.
     *
     * Do not call this method directly, use {@link DocIdSets#isBroken}.
     */
    public abstract boolean isBroken();

}
