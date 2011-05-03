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

package org.elasticsearch.search.facet.range;

import org.elasticsearch.search.facet.Facet;

import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public interface RangeFacet extends Facet, Iterable<RangeFacet.Entry> {

    /**
     * The type of the filter facet.
     */
    public static final String TYPE = "range";

    /**
     * An ordered list of range facet entries.
     */
    List<Entry> entries();

    /**
     * An ordered list of range facet entries.
     */
    List<Entry> getEntries();

    public class Entry {

        double from = Double.NEGATIVE_INFINITY;

        double to = Double.POSITIVE_INFINITY;

        String fromAsString;

        String toAsString;

        long count;
        long totalCount;

        double total;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        /**
         * Internal field used in facet collection
         */
        boolean foundInDoc;

        Entry() {
        }

        public double from() {
            return this.from;
        }

        public double getFrom() {
            return from();
        }

        public String fromAsString() {
            if (fromAsString != null) {
                return fromAsString;
            }
            return Double.toString(from);
        }

        public String getFromAsString() {
            return fromAsString();
        }

        public double to() {
            return this.to;
        }

        public double getTo() {
            return to();
        }

        public String toAsString() {
            if (toAsString != null) {
                return toAsString;
            }
            return Double.toString(to);
        }

        public String getToAsString() {
            return toAsString();
        }

        public long count() {
            return this.count;
        }

        public long getCount() {
            return count();
        }

        public long totalCount() {
            return this.totalCount;
        }

        public long getTotalCount() {
            return this.totalCount;
        }

        public double total() {
            return this.total;
        }

        public double getTotal() {
            return total();
        }

        /**
         * The mean of this facet interval.
         */
        public double mean() {
            return total / totalCount;
        }

        /**
         * The mean of this facet interval.
         */
        public double getMean() {
            return mean();
        }

        public double min() {
            return this.min;
        }

        public double getMin() {
            return this.min;
        }

        public double max() {
            return this.max;
        }

        public double getMax() {
            return this.max;
        }
    }
}
