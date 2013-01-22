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

package org.elasticsearch.search.facet.terms;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.facet.terms.doubles.TermsDoubleFacetCollector;
import org.elasticsearch.search.facet.terms.index.IndexNameFacetCollector;
import org.elasticsearch.search.facet.terms.longs.TermsLongFacetCollector;
import org.elasticsearch.search.facet.terms.strings.FieldsTermsStringFacetCollector;
import org.elasticsearch.search.facet.terms.strings.ScriptTermsStringFieldFacetCollector;
import org.elasticsearch.search.facet.terms.strings.TermsStringFacetCollector;
import org.elasticsearch.search.facet.terms.strings.TermsStringOrdinalsFacetCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 */
public class TermsFacetProcessor extends AbstractComponent implements FacetProcessor {

    @Inject
    public TermsFacetProcessor(Settings settings) {
        super(settings);
        InternalTermsFacet.registerStreams();
    }

    @Override
    public String[] types() {
        return new String[]{TermsFacet.TYPE};
    }

    @Override
    public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String field = null;
        int size = 10;

        String[] fieldsNames = null;
        ImmutableSet<BytesRef> excluded = ImmutableSet.of();
        String regex = null;
        String regexFlags = null;
        TermsFacet.ComparatorType comparatorType = TermsFacet.ComparatorType.COUNT;
        String scriptLang = null;
        String script = null;
        Map<String, Object> params = null;
        boolean allTerms = false;
        String executionHint = null;

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
                if ("exclude".equals(currentFieldName)) {
                    ImmutableSet.Builder<BytesRef> builder = ImmutableSet.builder();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        builder.add(parser.bytes());
                    }
                    excluded = builder.build();
                } else if ("fields".equals(currentFieldName)) {
                    List<String> fields = Lists.newArrayListWithCapacity(4);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(parser.text());
                    }
                    fieldsNames = fields.toArray(new String[fields.size()]);
                }
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("script_field".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("size".equals(currentFieldName)) {
                    size = parser.intValue();
                } else if ("all_terms".equals(currentFieldName) || "allTerms".equals(currentFieldName)) {
                    allTerms = parser.booleanValue();
                } else if ("regex".equals(currentFieldName)) {
                    regex = parser.text();
                } else if ("regex_flags".equals(currentFieldName) || "regexFlags".equals(currentFieldName)) {
                    regexFlags = parser.text();
                } else if ("order".equals(currentFieldName) || "comparator".equals(currentFieldName)) {
                    comparatorType = TermsFacet.ComparatorType.fromString(parser.text());
                } else if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("execution_hint".equals(currentFieldName) || "executionHint".equals(currentFieldName)) {
                    executionHint = parser.textOrNull();
                }
            }
        }

        if ("_index".equals(field)) {
            return new IndexNameFacetCollector(facetName, context.shardTarget().index(), comparatorType, size);
        }

        Pattern pattern = null;
        if (regex != null) {
            pattern = Regex.compile(regex, regexFlags);
        }

        SearchScript searchScript = null;
        if (script != null) {
            searchScript = context.scriptService().search(context.lookup(), scriptLang, script, params);
        }

        if (fieldsNames != null) {
            return new FieldsTermsStringFacetCollector(facetName, fieldsNames, size, comparatorType, allTerms, context, excluded, pattern, searchScript);
        }
        if (field == null && fieldsNames == null && script != null) {
            return new ScriptTermsStringFieldFacetCollector(facetName, size, comparatorType, context, excluded, pattern, scriptLang, script, params);
        }

        FieldMapper fieldMapper = context.smartNameFieldMapper(field);
        if (fieldMapper == null) {
            throw new FacetPhaseExecutionException(facetName, "failed to find mapping for [" + field + "]");
        }

        IndexFieldData indexFieldData = context.fieldData().getForField(fieldMapper);
        if (indexFieldData instanceof IndexNumericFieldData) {
            IndexNumericFieldData indexNumericFieldData = (IndexNumericFieldData) indexFieldData;
            if (indexNumericFieldData.getNumericType().isFloatingPoint()) {
                return new TermsDoubleFacetCollector(facetName, indexNumericFieldData, size, comparatorType, allTerms, context, excluded, searchScript);
            } else {
                return new TermsLongFacetCollector(facetName, indexNumericFieldData, size, comparatorType, allTerms, context, excluded, searchScript);
            }
        } else {
            if (script != null || "map".equals(executionHint)) {
                return new TermsStringFacetCollector(facetName, indexFieldData, size, comparatorType, allTerms, context, excluded, pattern, searchScript);
            } else if (indexFieldData instanceof IndexOrdinalFieldData) {
                return new TermsStringOrdinalsFacetCollector(facetName, (IndexOrdinalFieldData) indexFieldData, size, comparatorType, allTerms, context, excluded, pattern);
            } else {
                return new TermsStringFacetCollector(facetName, indexFieldData, size, comparatorType, allTerms, context, excluded, pattern, searchScript);
            }
        }
    }

    @Override
    public Facet reduce(String name, List<Facet> facets) {
        InternalTermsFacet first = (InternalTermsFacet) facets.get(0);
        return first.reduce(name, facets);
    }
}
