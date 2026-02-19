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
     * Returns the number of points that have been added to this digest.
     */
    long size();

    /**
     * Returns the fraction of all points added which are &le; x.
     */
    double cdf(double x);

    /**
     * Returns an estimate of a cutoff for a requested quantile.
     */
    double quantile(double q);

    /**
     * Returns centroids in ascending order by mean.
     */
    Collection<Centroid> centroids();

    /**
     * Returns the current compression factor.
     */
    double compression();

    /**
     * Returns the number of bytes required to encode this digest.
     */
    int byteSize();

    /**
     * Returns the current number of centroids.
     */
    int centroidCount();

    /**
     * Returns the minimum value seen.
     */
    double getMin();

    /**
     * Returns the maximum value seen.
     */
    double getMax();
}
