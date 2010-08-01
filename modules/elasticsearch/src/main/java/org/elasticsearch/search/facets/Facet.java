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

package org.elasticsearch.search.facets;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.search.facets.geodistance.GeoDistanceFacet;
import org.elasticsearch.search.facets.histogram.HistogramFacet;
import org.elasticsearch.search.facets.query.QueryFacet;
import org.elasticsearch.search.facets.statistical.StatisticalFacet;
import org.elasticsearch.search.facets.terms.TermsFacet;

/**
 * A search facet.
 *
 * @author kimchy (shay.banon)
 */
public interface Facet {

    /**
     * The type of the facet.
     */
    enum Type {
        /**
         * Terms facet type, matching {@link TermsFacet}.
         */
        TERMS(0, TermsFacet.class),
        /**
         * Query facet type, matching {@link QueryFacet}.
         */
        QUERY(1, QueryFacet.class),
        /**
         * Statistical facet type, matching {@link StatisticalFacet}.
         */
        STATISTICAL(2, StatisticalFacet.class),
        /**
         * Histogram facet type, matching {@link HistogramFacet}.
         */
        HISTOGRAM(3, HistogramFacet.class),
        /**
         * Geo Distance facet type, matching {@link GeoDistanceFacet}.
         */
        GEO_DISTANCE(4, GeoDistanceFacet.class);

        private int id;

        private Class<? extends Facet> type;

        Type(int id, Class<? extends Facet> type) {
            this.id = id;
            this.type = type;
        }

        public int id() {
            return id;
        }

        /**
         * The facet class type.
         */
        public Class<? extends Facet> type() {
            return this.type;
        }

        /**
         * The facet class type.
         */
        public Class<? extends Facet> getType() {
            return type();
        }

        public static Type fromId(int id) {
            if (id == 0) {
                return TERMS;
            } else if (id == 1) {
                return QUERY;
            } else if (id == 2) {
                return STATISTICAL;
            } else if (id == 3) {
                return HISTOGRAM;
            } else if (id == 4) {
                return GEO_DISTANCE;
            } else {
                throw new ElasticSearchIllegalArgumentException("No match for id [" + id + "]");
            }
        }
    }

    /**
     * The "logical" name of the search facet.
     */
    String name();

    /**
     * The "logical" name of the search facet.
     */
    String getName();

    /**
     * The type of the facet.
     */
    Type type();

    /**
     * The type of the facet.
     */
    Type getType();
}
