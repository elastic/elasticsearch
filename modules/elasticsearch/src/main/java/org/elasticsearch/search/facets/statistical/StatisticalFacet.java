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

package org.elasticsearch.search.facets.statistical;

import org.elasticsearch.search.facets.Facet;

/**
 * Numeric statistical information.
 *
 * @author kimchy (shay.banon)
 */
public interface StatisticalFacet extends Facet {

    /**
     * The number of values counted.
     */
    long count();

    /**
     * The number of values counted.
     */
    long getCount();

    /**
     * The total (sum) of values.
     */
    double total();

    /**
     * The total (sum) of values.
     */
    double getTotal();

    /**
     * The mean (average) of the values.
     */
    double mean();

    /**
     * The mean (average) of the values.
     */
    double getMean();

    /**
     * The minimum value.
     */
    double min();

    /**
     * The minimum value.
     */
    double getMin();

    /**
     * The maximum value.
     */
    double max();

    /**
     * The maximum value.
     */
    double getMax();
}
