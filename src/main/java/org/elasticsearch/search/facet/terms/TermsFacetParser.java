/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.search.facet.terms;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.facet.terms.doubles.TermsDoubleFacetExecutor;
import org.elasticsearch.search.facet.terms.longs.TermsLongFacetExecutor;
import org.elasticsearch.search.facet.terms.strings.FieldsTermsStringFacetExecutor;
import org.elasticsearch.search.facet.terms.strings.ScriptTermsStringFieldFacetExecutor;
import org.elasticsearch.search.facet.terms.strings.TermsStringFacetExecutor;
import org.elasticsearch.search.facet.terms.strings.TermsStringOrdinalsFacetExecutor;
import org.elasticsearch.search.facet.terms.unmapped.UnmappedFieldExecutor;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 */
public class TermsFacetParser extends AbstractComponent implements FacetParser {

    private final int ordinalsCacheAbove;

    @Inject
    public TermsFacetParser(Settings settings) {
        super(settings);
        InternalTermsFacet.registerStreams();
        this.ordinalsCacheAbove = componentSettings.getAsInt("ordinals_cache_above", 10000); // above 40k we want to cache
    }

    @Override
    public String[] types() {
        return new String[]{TermsFacet.TYPE};
    }

    @Override
    public FacetExecutor.Mode defaultMainMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor.Mode defaultGlobalMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String field = null;
        int size = 10;
        int shardSize = -1;

        String[] fieldsNames = null;
        ImmutableSet<BytesRef> excluded = ImmutableSet.of();
        String regex = null;
        String regexFlags = null;
        TermsFacet.ComparatorType comparatorType = TermsFacet.ComparatorType.COUNT;
        String scriptLang = null;
        String script = null;
        ScriptService.ScriptType scriptType = null;
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
                } else {
                    throw new ElasticsearchParseException("unknown parameter [" + currentFieldName + "] while parsing terms facet [" + facetName + "]");
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
                } else {
                    throw new ElasticsearchParseException("unknown parameter [" + currentFieldName + "] while parsing terms facet [" + facetName + "]");
                }
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if (ScriptService.SCRIPT_INLINE.match(currentFieldName)) {
                    script = parser.text();
                    scriptType = ScriptService.ScriptType.INLINE;
                } else if (ScriptService.SCRIPT_ID.match(currentFieldName)) {
                    script = parser.text();
                    scriptType = ScriptService.ScriptType.INDEXED;
                } else if (ScriptService.SCRIPT_FILE.match(currentFieldName)) {
                    script = parser.text();
                    scriptType = ScriptService.ScriptType.FILE;
                } else if (ScriptService.SCRIPT_LANG.match(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("size".equals(currentFieldName)) {
                    size = parser.intValue();
                } else if ("shard_size".equals(currentFieldName) || "shardSize".equals(currentFieldName)) {
                    shardSize = parser.intValue();
                } else if ("all_terms".equals(currentFieldName) || "allTerms".equals(currentFieldName)) {
                    allTerms = parser.booleanValue();
                } else if ("regex".equals(currentFieldName)) {
                    regex = parser.text();
                } else if ("regex_flags".equals(currentFieldName) || "regexFlags".equals(currentFieldName)) {
                    regexFlags = parser.text();
                } else if ("order".equals(currentFieldName) || "comparator".equals(currentFieldName)) {
                    comparatorType = TermsFacet.ComparatorType.fromString(parser.text());
                } else if ("execution_hint".equals(currentFieldName) || "executionHint".equals(currentFieldName)) {
                    executionHint = parser.textOrNull();
                } else {
                    throw new ElasticsearchParseException("unknown parameter [" + currentFieldName + "] while parsing terms facet [" + facetName + "]");
                }
            }
        }

        if (fieldsNames != null && fieldsNames.length == 1) {
            field = fieldsNames[0];
            fieldsNames = null;
        }

        Pattern pattern = null;
        if (regex != null) {
            pattern = Regex.compile(regex, regexFlags);
        }

        SearchScript searchScript = null;
        if (script != null) {
            searchScript = context.scriptService().search(context.lookup(), scriptLang, script, scriptType, params);
        }

        // shard_size cannot be smaller than size as we need to at least fetch <size> entries from every shards in order to return <size>
        if (shardSize < size) {
            shardSize = size;
        }

        if (fieldsNames != null) {

            // in case of multi files, we only collect the fields that are mapped and facet on them.
            ArrayList<FieldMapper> mappers = new ArrayList<>(fieldsNames.length);
            for (int i = 0; i < fieldsNames.length; i++) {
                FieldMapper mapper = context.smartNameFieldMapper(fieldsNames[i]);
                if (mapper != null) {
                    mappers.add(mapper);
                }
            }
            if (mappers.isEmpty()) {
                // non of the fields is mapped
                return new UnmappedFieldExecutor(size, comparatorType);
            }
            return new FieldsTermsStringFacetExecutor(mappers.toArray(new FieldMapper[mappers.size()]), size, shardSize, comparatorType, allTerms, context, excluded, pattern, searchScript);
        }
        if (field == null && script != null) {
            return new ScriptTermsStringFieldFacetExecutor(size, shardSize, comparatorType, context, excluded, pattern, scriptLang, script, scriptType, params, context.cacheRecycler());
        }

        if (field == null) {
            throw new ElasticsearchParseException("terms facet [" + facetName + "] must have a field, fields or script parameter");
        }

        FieldMapper fieldMapper = context.smartNameFieldMapper(field);
        if (fieldMapper == null) {
            return new UnmappedFieldExecutor(size, comparatorType);
        }

        IndexFieldData indexFieldData = context.fieldData().getForField(fieldMapper);
        if (indexFieldData instanceof IndexNumericFieldData) {
            IndexNumericFieldData indexNumericFieldData = (IndexNumericFieldData) indexFieldData;
            if (indexNumericFieldData.getNumericType().isFloatingPoint()) {
                return new TermsDoubleFacetExecutor(indexNumericFieldData, size, shardSize, comparatorType, allTerms, context, excluded, searchScript, context.cacheRecycler());
            } else {
                return new TermsLongFacetExecutor(indexNumericFieldData, size, shardSize, comparatorType, allTerms, context, excluded, searchScript, context.cacheRecycler());
            }
        } else {
            if (script != null || "map".equals(executionHint)) {
                return new TermsStringFacetExecutor(indexFieldData, size, shardSize, comparatorType, allTerms, context, excluded, pattern, searchScript);
            } else if (indexFieldData instanceof IndexFieldData.WithOrdinals) {
                return new TermsStringOrdinalsFacetExecutor((IndexFieldData.WithOrdinals) indexFieldData, size, shardSize, comparatorType, allTerms, context, excluded, pattern, ordinalsCacheAbove);
            } else {
                return new TermsStringFacetExecutor(indexFieldData, size, shardSize, comparatorType, allTerms, context, excluded, pattern, searchScript);
            }
        }
    }
}
