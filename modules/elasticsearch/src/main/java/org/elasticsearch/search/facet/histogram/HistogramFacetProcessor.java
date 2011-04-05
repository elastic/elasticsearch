/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.facet.histogram;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.facet.histogram.bounded.BoundedCountHistogramFacetCollector;
import org.elasticsearch.search.facet.histogram.bounded.BoundedValueHistogramFacetCollector;
import org.elasticsearch.search.facet.histogram.bounded.BoundedValueScriptHistogramFacetCollector;
import org.elasticsearch.search.facet.histogram.unbounded.*;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class HistogramFacetProcessor extends AbstractComponent implements FacetProcessor {

    @Inject public HistogramFacetProcessor(Settings settings) {
        super(settings);
        InternalHistogramFacet.registerStreams();
    }

    @Override public String[] types() {
        return new String[]{HistogramFacet.TYPE};
    }

    @Override public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String keyField = null;
        String valueField = null;
        String keyScript = null;
        String valueScript = null;
        String scriptLang = null;
        Map<String, Object> params = null;
        long interval = 0;
        HistogramFacet.ComparatorType comparatorType = HistogramFacet.ComparatorType.KEY;
        XContentParser.Token token;
        String fieldName = null;
        String sFrom = null;
        String sTo = null;
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
                } else if ("from".equals(fieldName)) {
                    sFrom = parser.text();
                } else if ("to".equals(fieldName)) {
                    sTo = parser.text();
                } else if ("time_interval".equals(fieldName)) {
                    interval = TimeValue.parseTimeValue(parser.text(), null).millis();
                } else if ("key_script".equals(fieldName) || "keyScript".equals(fieldName)) {
                    keyScript = parser.text();
                } else if ("value_script".equals(fieldName) || "valueScript".equals(fieldName)) {
                    valueScript = parser.text();
                } else if ("order".equals(fieldName) || "comparator".equals(fieldName)) {
                    comparatorType = HistogramFacet.ComparatorType.fromString(parser.text());
                } else if ("lang".equals(fieldName)) {
                    scriptLang = parser.text();
                }
            }
        }

        if (keyScript != null && valueScript != null) {
            return new ScriptHistogramFacetCollector(facetName, scriptLang, keyScript, valueScript, params, interval, comparatorType, context);
        }

        if (keyField == null) {
            throw new FacetPhaseExecutionException(facetName, "key field is required to be set for histogram facet, either using [field] or using [key_field]");
        }

        if (interval <= 0) {
            throw new FacetPhaseExecutionException(facetName, "[interval] is required to be set for histogram facet");
        }

        if (sFrom != null && sTo != null && keyField != null) {
            FieldMapper mapper = context.mapperService().smartNameFieldMapper(keyField);
            if (mapper == null) {
                throw new FacetPhaseExecutionException(facetName, "No mapping found for key_field [" + keyField + "]");
            }
            long from = ((Number) mapper.valueFromString(sFrom)).longValue();
            long to = ((Number) mapper.valueFromString(sTo)).longValue();

            if (valueField != null) {
                return new BoundedValueHistogramFacetCollector(facetName, keyField, valueField, interval, from, to, comparatorType, context);
            } else if (valueScript != null) {
                return new BoundedValueScriptHistogramFacetCollector(facetName, keyField, scriptLang, valueScript, params, interval, from, to, comparatorType, context);
            } else {
                return new BoundedCountHistogramFacetCollector(facetName, keyField, interval, from, to, comparatorType, context);
            }
        }

        if (valueScript != null) {
            return new ValueScriptHistogramFacetCollector(facetName, keyField, scriptLang, valueScript, params, interval, comparatorType, context);
        } else if (valueField == null) {
            return new CountHistogramFacetCollector(facetName, keyField, interval, comparatorType, context);
        } else if (keyField.equals(valueField)) {
            return new FullHistogramFacetCollector(facetName, keyField, interval, comparatorType, context);
        } else {
            // we have a value field, and its different than the key
            return new ValueHistogramFacetCollector(facetName, keyField, valueField, interval, comparatorType, context);
        }
    }

    @Override public Facet reduce(String name, List<Facet> facets) {
        InternalHistogramFacet first = (InternalHistogramFacet) facets.get(0);
        return first.reduce(name, facets);
    }
}
