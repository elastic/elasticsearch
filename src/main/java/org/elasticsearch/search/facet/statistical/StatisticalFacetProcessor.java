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

package org.elasticsearch.search.facet.statistical;

import com.google.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
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
 *
 */
public class StatisticalFacetProcessor extends AbstractComponent implements FacetProcessor {

    @Inject
    public StatisticalFacetProcessor(Settings settings) {
        super(settings);
        InternalStatisticalFacet.registerStreams();
    }

    @Override
    public String[] types() {
        return new String[]{StatisticalFacet.TYPE};
    }

    @Override
    public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String field = null;
        String[] fieldsNames = null;

        String script = null;
        String scriptLang = null;
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
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("fields".equals(currentFieldName)) {
                    List<String> fields = Lists.newArrayListWithCapacity(4);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(parser.text());
                    }
                    fieldsNames = fields.toArray(new String[fields.size()]);
                }
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                }
            }
        }
        if (fieldsNames != null) {
            IndexNumericFieldData[] indexFieldDatas = new IndexNumericFieldData[fieldsNames.length];
            for (int i = 0; i < fieldsNames.length; i++) {
                FieldMapper fieldMapper = context.smartNameFieldMapper(fieldsNames[i]);
                if (fieldMapper == null) {
                    throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + fieldsNames[i] + "]");
                }
                indexFieldDatas[i] = context.fieldData().getForField(fieldMapper);
            }
            return new StatisticalFieldsFacetCollector(facetName, indexFieldDatas, context);
        }
        if (script == null && field == null) {
            throw new FacetPhaseExecutionException(facetName, "statistical facet requires either [script] or [field] to be set");
        }
        if (field != null) {
            FieldMapper fieldMapper = context.smartNameFieldMapper(field);
            if (fieldMapper == null) {
                throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + field + "]");
            }
            IndexNumericFieldData indexFieldData = context.fieldData().getForField(fieldMapper);
            return new StatisticalFacetCollector(facetName, indexFieldData, context);
        } else {
            return new ScriptStatisticalFacetCollector(facetName, scriptLang, script, params, context);
        }
    }

    @Override
    public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            return facets.get(0);
        }
        double min = Double.NaN;
        double max = Double.NaN;
        double total = 0;
        double sumOfSquares = 0;
        long count = 0;

        for (Facet facet : facets) {
            if (!facet.name().equals(name)) {
                continue;
            }
            InternalStatisticalFacet statsFacet = (InternalStatisticalFacet) facet;
            if (statsFacet.min() < min || Double.isNaN(min)) {
                min = statsFacet.min();
            }
            if (statsFacet.max() > max || Double.isNaN(max)) {
                max = statsFacet.max();
            }
            total += statsFacet.total();
            sumOfSquares += statsFacet.sumOfSquares();
            count += statsFacet.count();
        }

        return new InternalStatisticalFacet(name, min, max, total, sumOfSquares, count);
    }
}
