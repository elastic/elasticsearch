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

package org.elasticsearch.search.facet.range;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class RangeFacetProcessor extends AbstractComponent implements FacetProcessor {

    @Inject public RangeFacetProcessor(Settings settings) {
        super(settings);
        InternalRangeFacet.registerStreams();
    }

    @Override public String[] types() {
        return new String[]{RangeFacet.TYPE};
    }

    @Override public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String keyField = null;
        String valueField = null;
        String scriptLang = null;
        String keyScript = null;
        String valueScript = null;
        Map<String, Object> params = null;
        XContentParser.Token token;
        String fieldName = null;
        List<RangeFacet.Entry> entries = Lists.newArrayList();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (!"ranges".equals(fieldName)) {
                    // this is the actual field name, so also update the keyField
                    keyField = fieldName;
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    RangeFacet.Entry entry = new RangeFacet.Entry();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if ("from".equals(fieldName)) {
                                entry.fromAsString = parser.text();
                            } else if ("to".equals(fieldName)) {
                                entry.toAsString = parser.text();
                            }
                        } else if (token.isValue()) {
                            if ("from".equals(fieldName)) {
                                entry.from = parser.doubleValue();
                            } else if ("to".equals(fieldName)) {
                                entry.to = parser.doubleValue();
                            }
                        }
                    }
                    entries.add(entry);
                }
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
                } else if ("key_script".equals(fieldName) || "keyScript".equals(fieldName)) {
                    keyScript = parser.text();
                } else if ("value_script".equals(fieldName) || "valueScript".equals(fieldName)) {
                    valueScript = parser.text();
                } else if ("lang".equals(fieldName)) {
                    scriptLang = parser.text();
                }
            }
        }

        if (entries.isEmpty()) {
            throw new FacetPhaseExecutionException(facetName, "no ranges defined for range facet");
        }

        RangeFacet.Entry[] rangeEntries = entries.toArray(new RangeFacet.Entry[entries.size()]);

        // fix the range entries if needed
        if (keyField != null) {
            FieldMapper mapper = context.mapperService().smartNameFieldMapper(keyField);
            if (mapper == null) {
                throw new FacetPhaseExecutionException(facetName, "No mapping found for key_field [" + keyField + "]");
            }
            for (RangeFacet.Entry entry : rangeEntries) {
                if (entry.fromAsString != null) {
                    entry.from = ((Number) mapper.valueFromString(entry.fromAsString)).doubleValue();
                }
                if (entry.toAsString != null) {
                    entry.to = ((Number) mapper.valueFromString(entry.toAsString)).doubleValue();
                }
            }
        }

        if (keyScript != null && valueScript != null) {
            return new ScriptRangeFacetCollector(facetName, scriptLang, keyScript, valueScript, params, rangeEntries, context);
        }

        if (keyField == null) {
            throw new FacetPhaseExecutionException(facetName, "key field is required to be set for range facet, either using [field] or using [key_field]");
        }

        if (valueField == null || keyField.equals(valueField)) {
            return new RangeFacetCollector(facetName, keyField, rangeEntries, context);
        } else {
            // we have a value field, and its different than the key
            return new KeyValueRangeFacetCollector(facetName, keyField, valueField, rangeEntries, context);
        }
    }

    @Override public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            return facets.get(0);
        }
        InternalRangeFacet agg = null;
        for (Facet facet : facets) {
            InternalRangeFacet geoDistanceFacet = (InternalRangeFacet) facet;
            if (agg == null) {
                agg = geoDistanceFacet;
            } else {
                for (int i = 0; i < geoDistanceFacet.entries.length; i++) {
                    RangeFacet.Entry aggEntry = agg.entries[i];
                    RangeFacet.Entry currentEntry = geoDistanceFacet.entries[i];
                    aggEntry.count += currentEntry.count;
                    aggEntry.totalCount += currentEntry.totalCount;
                    aggEntry.total += currentEntry.total;
                    if (currentEntry.min < aggEntry.min) {
                        aggEntry.min = currentEntry.min;
                    }
                    if (currentEntry.max > aggEntry.max) {
                        aggEntry.max = currentEntry.max;
                    }
                }
            }
        }
        return agg;
    }
}
