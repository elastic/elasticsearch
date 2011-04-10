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

import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.xcontent.ip.IpFieldMapper;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.facet.terms.bytes.TermsByteFacetCollector;
import org.elasticsearch.search.facet.terms.bytes.TermsByteOrdinalsFacetCollector;
import org.elasticsearch.search.facet.terms.doubles.TermsDoubleFacetCollector;
import org.elasticsearch.search.facet.terms.doubles.TermsDoubleOrdinalsFacetCollector;
import org.elasticsearch.search.facet.terms.floats.TermsFloatFacetCollector;
import org.elasticsearch.search.facet.terms.floats.TermsFloatOrdinalsFacetCollector;
import org.elasticsearch.search.facet.terms.index.IndexNameFacetCollector;
import org.elasticsearch.search.facet.terms.ints.TermsIntFacetCollector;
import org.elasticsearch.search.facet.terms.ints.TermsIntOrdinalsFacetCollector;
import org.elasticsearch.search.facet.terms.ip.TermsIpFacetCollector;
import org.elasticsearch.search.facet.terms.ip.TermsIpOrdinalsFacetCollector;
import org.elasticsearch.search.facet.terms.longs.TermsLongFacetCollector;
import org.elasticsearch.search.facet.terms.longs.TermsLongOrdinalsFacetCollector;
import org.elasticsearch.search.facet.terms.shorts.TermsShortFacetCollector;
import org.elasticsearch.search.facet.terms.shorts.TermsShortOrdinalsFacetCollector;
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
 * @author kimchy (shay.banon)
 */
public class TermsFacetProcessor extends AbstractComponent implements FacetProcessor {

    @Inject public TermsFacetProcessor(Settings settings) {
        super(settings);
        InternalTermsFacet.registerStreams();
    }

    @Override public String[] types() {
        return new String[]{TermsFacet.TYPE};
    }

    @Override public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String field = null;
        int size = 10;

        String[] fieldsNames = null;
        ImmutableSet<String> excluded = ImmutableSet.of();
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
                    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        builder.add(parser.text());
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
        if (fieldsNames != null) {
            return new FieldsTermsStringFacetCollector(facetName, fieldsNames, size, comparatorType, allTerms, context, excluded, pattern, scriptLang, script, params);
        }
        if (field == null && fieldsNames == null && script != null) {
            return new ScriptTermsStringFieldFacetCollector(facetName, size, comparatorType, context, excluded, pattern, scriptLang, script, params);
        }

        FieldMapper fieldMapper = context.mapperService().smartNameFieldMapper(field);
        if (fieldMapper != null) {
            if (fieldMapper instanceof IpFieldMapper) {
                if (script != null || "map".equals(executionHint)) {
                    return new TermsIpFacetCollector(facetName, field, size, comparatorType, allTerms, context, scriptLang, script, params);
                } else {
                    return new TermsIpOrdinalsFacetCollector(facetName, field, size, comparatorType, allTerms, context, null);
                }
            } else if (fieldMapper.fieldDataType() == FieldDataType.DefaultTypes.LONG) {
                if (script != null || "map".equals(executionHint)) {
                    return new TermsLongFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded, scriptLang, script, params);
                } else {
                    return new TermsLongOrdinalsFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded);
                }
            } else if (fieldMapper.fieldDataType() == FieldDataType.DefaultTypes.DOUBLE) {
                if (script != null) {
                    return new TermsDoubleFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded, scriptLang, script, params);
                } else {
                    return new TermsDoubleOrdinalsFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded);
                }
            } else if (fieldMapper.fieldDataType() == FieldDataType.DefaultTypes.INT) {
                if (script != null || "map".equals(executionHint)) {
                    return new TermsIntFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded, scriptLang, script, params);
                } else {
                    return new TermsIntOrdinalsFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded);
                }
            } else if (fieldMapper.fieldDataType() == FieldDataType.DefaultTypes.FLOAT) {
                if (script != null || "map".equals(executionHint)) {
                    return new TermsFloatFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded, scriptLang, script, params);
                } else {
                    return new TermsFloatOrdinalsFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded);
                }
            } else if (fieldMapper.fieldDataType() == FieldDataType.DefaultTypes.SHORT) {
                if (script != null || "map".equals(executionHint)) {
                    return new TermsShortFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded, scriptLang, script, params);
                } else {
                    return new TermsShortOrdinalsFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded);
                }
            } else if (fieldMapper.fieldDataType() == FieldDataType.DefaultTypes.BYTE) {
                if (script != null || "map".equals(executionHint)) {
                    return new TermsByteFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded, scriptLang, script, params);
                } else {
                    return new TermsByteOrdinalsFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded);
                }
            } else if (fieldMapper.fieldDataType() == FieldDataType.DefaultTypes.STRING) {
                if (script == null && !"map".equals(executionHint)) {
                    return new TermsStringOrdinalsFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded, pattern);
                }
            }
        }
        return new TermsStringFacetCollector(facetName, field, size, comparatorType, allTerms, context, excluded, pattern, scriptLang, script, params);
    }

    @Override public Facet reduce(String name, List<Facet> facets) {
        InternalTermsFacet first = (InternalTermsFacet) facets.get(0);
        return first.reduce(name, facets);
    }
}
