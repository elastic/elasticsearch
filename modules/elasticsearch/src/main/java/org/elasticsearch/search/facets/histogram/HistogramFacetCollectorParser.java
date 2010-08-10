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

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.facets.FacetPhaseExecutionException;
import org.elasticsearch.search.facets.collector.FacetCollector;
import org.elasticsearch.search.facets.collector.FacetCollectorParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class HistogramFacetCollectorParser implements FacetCollectorParser {

    public static final String NAME = "histogram";

    @Override public String[] names() {
        return new String[]{NAME};
    }

    @Override public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String keyField = null;
        String valueField = null;
        String keyScript = null;
        String valueScript = null;
        Map<String, Object> params = null;
        long interval = 0;
        HistogramFacet.ComparatorType comparatorType = HistogramFacet.ComparatorType.KEY;
        XContentParser.Token token;
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(fieldName)) {
                    params = parser.map();
                }
            } else if (token.isValue()) {
                if ("field".equals(fieldName)) {
                    keyField = parser.text();
                } else if ("key_field".equals(fieldName) || "keyField".equals(fieldName)) {
                    keyField = parser.text();
                } else if ("value_field".equals(fieldName) || "valueField".equals(fieldName)) {
                    valueField = parser.text();
                } else if ("interval".equals(fieldName)) {
                    interval = parser.longValue();
                } else if ("time_interval".equals(fieldName)) {
                    interval = TimeValue.parseTimeValue(parser.text(), null).millis();
                } else if ("key_script".equals(fieldName) || "keyScript".equals(fieldName)) {
                    keyScript = parser.text();
                } else if ("value_script".equals(fieldName) || "valueScript".equals(fieldName)) {
                    valueScript = parser.text();
                } else if ("order".equals(fieldName) || "comparator".equals(fieldName)) {
                    comparatorType = HistogramFacet.ComparatorType.fromString(parser.text());
                }
            }
        }

        if (keyScript != null && valueScript != null) {
            return new ScriptHistogramFacetCollector(facetName, keyScript, valueScript, params, interval, comparatorType, context);
        }

        if (keyField == null) {
            throw new FacetPhaseExecutionException(facetName, "key field is required to be set for histogram facet, either using [field] or using [key_field]");
        }

        if (interval <= 0) {
            throw new FacetPhaseExecutionException(facetName, "[interval] is required to be set for histogram facet");
        }

        if (interval < 0) {
            throw new FacetPhaseExecutionException(facetName, "[interval] is required to be positive for histogram facet");
        }

        if (valueField == null || keyField.equals(valueField)) {
            return new HistogramFacetCollector(facetName, keyField, interval, comparatorType, context);
        } else {
            // we have a value field, and its different than the key
            return new KeyValueHistogramFacetCollector(facetName, keyField, valueField, interval, comparatorType, context);
        }
    }
}