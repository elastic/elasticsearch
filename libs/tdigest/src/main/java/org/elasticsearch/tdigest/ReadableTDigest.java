/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import java.util.Collection;

/**
 * Read-only view of a T-Digest.
 */
public interface ReadableTDigest {

    /**
        * Returns the number of points that have been added to this TDigest.
     *
         * @return The sum of the weights on all centroids.
        */
    long size();

    /**
     * A {@link Collection} that lets you go through the centroids in ascending order by mean.  Centroids
     * returned will not be re-used, but may or may not share storage with this TDigest.
     *
     * @return The centroids in the form of a Collection.
     */
    Collection<Centroid> centroids();

    /**
     * Returns the current number of centroids.
     */
    int centroidCount();

    /**
     * Returns the minimum value seen. Returns NaN if this digest is empty.
     */
    double getMin();

    /**
     * Returns the maximum value seen. Returns NaN if this digest is empty.
     */
    double getMax();
}
