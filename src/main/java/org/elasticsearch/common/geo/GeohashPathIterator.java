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

package org.elasticsearch.common.geo;

import java.util.Iterator;

/**
 * This class iterates over the cells of a given geohash. Assume geohashes
 * form a tree, this iterator traverses this tree form a leaf (actual gehash)
 * to the root (geohash of length 1). 
 */
public final class GeohashPathIterator implements Iterator<String> {

    private final String geohash;
    private int currentLength;

    /**
     * Create a new {@link GeohashPathIterator} for a given geohash
     * @param geohash The geohash to traverse
     */
    public GeohashPathIterator(String geohash) {
        this.geohash = geohash;
        this.currentLength = geohash.length();
    }

    @Override
    public boolean hasNext() {
        return currentLength > 0;
    }

    @Override
    public String next() {
        String result = geohash.substring(0, currentLength);
        currentLength--;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("unable to remove a geohash from this path");
    }
}