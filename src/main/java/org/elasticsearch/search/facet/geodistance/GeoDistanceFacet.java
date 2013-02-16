/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.search.facet.geodistance;

import org.elasticsearch.search.facet.Facet;

import java.util.List;

/**
 *
 */
public interface GeoDistanceFacet extends Facet, Iterable<GeoDistanceFacet.Entry> {

    /**
     * The type of the filter facet.
     */
    public static final String TYPE = "geo_distance";

    /**
     * An ordered list of geo distance facet entries.
     */
    List<Entry> getEntries();

    public class Entry {

        double from = Double.NEGATIVE_INFINITY;
        double to = Double.POSITIVE_INFINITY;
        long count;
        long totalCount;
        double total;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        /**
         * internal field used to see if this entry was already found for a doc
         */
        boolean foundInDoc = false;

        Entry() {
        }

        public Entry(double from, double to, long count, long totalCount, double total, double min, double max) {
            this.from = from;
            this.to = to;
            this.count = count;
            this.totalCount = totalCount;
            this.total = total;
            this.min = min;
            this.max = max;
        }

        public double getFrom() {
            return this.from;
        }

        public double getTo() {
            return this.to;
        }

        public long getCount() {
            return this.count;
        }

        public long getTotalCount() {
            return this.totalCount;
        }

        public double getTotal() {
            return this.total;
        }

        /**
         * The mean of this facet interval.
         */
        public double getMean() {
            if (totalCount == 0) {
                return 0;
            }
            return total / totalCount;
        }

        public double getMin() {
            return this.min;
        }

        public double getMax() {
            return this.max;
        }
    }
}
