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

package org.elasticsearch.search.facets.histogram;

import org.elasticsearch.search.facets.FacetPhaseExecutionException;
import org.elasticsearch.search.facets.collector.FacetCollector;
import org.elasticsearch.search.facets.collector.FacetCollectorParser;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.util.xcontent.XContentParser;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class HistogramFacetCollectorParser implements FacetCollectorParser {

    public static final String NAME = "histogram";

    @Override public String name() {
        return NAME;
    }

    @Override public FacetCollector parser(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String field = null;

        String fieldName = null;
        XContentParser.Token token;
        long interval = -1;
        HistogramFacet.ComparatorType comparatorType = HistogramFacet.ComparatorType.VALUE;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("field".equals(fieldName)) {
                    field = parser.text();
                } else if ("interval".equals(fieldName)) {
                    interval = parser.longValue();
                } else if ("comparator".equals(fieldName)) {
                    comparatorType = HistogramFacet.ComparatorType.fromString(parser.text());
                }
            }
        }

        if (interval == -1) {
            throw new FacetPhaseExecutionException(facetName, "[interval] is required to be set for histogram facet");
        }

        if (interval < 0) {
            throw new FacetPhaseExecutionException(facetName, "[interval] is required to be positive for histogram facet");
        }

        return new HistogramFacetCollector(facetName, field, interval, comparatorType, context.fieldDataCache(), context.mapperService());
    }
}