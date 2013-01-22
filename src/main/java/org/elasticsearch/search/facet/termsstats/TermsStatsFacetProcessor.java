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

package org.elasticsearch.search.facet.termsstats;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.facet.termsstats.doubles.TermsStatsDoubleFacetCollector;
import org.elasticsearch.search.facet.termsstats.longs.TermsStatsLongFacetCollector;
import org.elasticsearch.search.facet.termsstats.strings.TermsStatsStringFacetCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TermsStatsFacetProcessor extends AbstractComponent implements FacetProcessor {

    @Inject
    public TermsStatsFacetProcessor(Settings settings) {
        super(settings);
        InternalTermsStatsFacet.registerStreams();
    }

    @Override
    public String[] types() {
        return new String[]{TermsStatsFacet.TYPE, "termsStats"};
    }

    @Override
    public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String keyField = null;
        String valueField = null;
        int size = 10;
        TermsStatsFacet.ComparatorType comparatorType = TermsStatsFacet.ComparatorType.COUNT;
        String scriptLang = null;
        String script = null;
        Map<String, Object> params = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    params = parser.map();
                }
            } else if (token.isValue()) {
                if ("key_field".equals(currentFieldName) || "keyField".equals(currentFieldName)) {
                    keyField = parser.text();
                } else if ("value_field".equals(currentFieldName) || "valueField".equals(currentFieldName)) {
                    valueField = parser.text();
                } else if ("script_field".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("value_script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("size".equals(currentFieldName)) {
                    size = parser.intValue();
                } else if ("all_terms".equals(currentFieldName) || "allTerms".equals(currentFieldName)) {
                    if (parser.booleanValue()) {
                        size = 0; // indicates all terms
                    }
                } else if ("order".equals(currentFieldName) || "comparator".equals(currentFieldName)) {
                    comparatorType = TermsStatsFacet.ComparatorType.fromString(parser.text());
                } else if ("value_script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                }
            }
        }

        if (keyField == null) {
            throw new FacetPhaseExecutionException(facetName, "[key_field] is required to be set for terms stats facet");
        }
        if (valueField == null && script == null) {
            throw new FacetPhaseExecutionException(facetName, "either [value_field] or [script] are required to be set for terms stats facet");
        }

        FieldMapper keyMapper = context.smartNameFieldMapper(keyField);
        if (keyMapper == null) {
            throw new FacetPhaseExecutionException(facetName, "failed to find mapping for " + keyField);
        }
        IndexFieldData keyIndexFieldData = context.fieldData().getForField(keyMapper);

        IndexNumericFieldData valueIndexFieldData = null;
        SearchScript valueScript = null;
        if (valueField != null) {
            valueIndexFieldData = context.fieldData().getForField(context.smartNameFieldMapper(valueField));
        } else {
            valueScript = context.scriptService().search(context.lookup(), scriptLang, script, params);
        }

        if (keyIndexFieldData instanceof IndexNumericFieldData) {
            IndexNumericFieldData keyIndexNumericFieldData = (IndexNumericFieldData) keyIndexFieldData;
            if (keyIndexNumericFieldData.getNumericType().isFloatingPoint()) {
                return new TermsStatsDoubleFacetCollector(facetName, keyIndexNumericFieldData, valueIndexFieldData, valueScript, size, comparatorType, context);
            } else {
                return new TermsStatsLongFacetCollector(facetName, keyIndexNumericFieldData, valueIndexFieldData, valueScript, size, comparatorType, context);
            }
        }
        return new TermsStatsStringFacetCollector(facetName, keyIndexFieldData, valueIndexFieldData, valueScript, size, comparatorType, context);
    }

    @Override
    public Facet reduce(String name, List<Facet> facets) {
        InternalTermsStatsFacet first = (InternalTermsStatsFacet) facets.get(0);
        return first.reduce(name, facets);
    }
}