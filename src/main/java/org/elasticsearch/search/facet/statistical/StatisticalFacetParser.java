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
package org.elasticsearch.search.facet.statistical;

import com.google.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class StatisticalFacetParser extends AbstractComponent implements FacetParser {

    @Inject
    public StatisticalFacetParser(Settings settings) {
        super(settings);
        InternalStatisticalFacet.registerStreams();
    }

    @Override
    public String[] types() {
        return new String[]{StatisticalFacet.TYPE};
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
        String[] fieldsNames = null;

        String script = null;
        String scriptLang = null;
        ScriptService.ScriptType scriptType = null;
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
                    scriptType = ScriptService.ScriptType.INLINE;
                } else if ("script_id".equals(currentFieldName)) {
                    script = parser.text();
                    scriptType = ScriptService.ScriptType.INDEXED;
                } else if ("file".equals(currentFieldName)) {
                    script = parser.text();
                    scriptType = ScriptService.ScriptType.FILE;
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
                if (!(fieldMapper instanceof NumberFieldMapper)) {
                    throw new FacetPhaseExecutionException(facetName, "field [" + field + "] isn't a number field, but a " + fieldMapper.fieldDataType().getType());
                }
                indexFieldDatas[i] = context.fieldData().getForField(fieldMapper);
            }
            return new StatisticalFieldsFacetExecutor(indexFieldDatas, context);
        }
        if (script == null && field == null) {
            throw new FacetPhaseExecutionException(facetName, "statistical facet requires either [script] or [field] to be set");
        }
        if (field != null) {
            FieldMapper fieldMapper = context.smartNameFieldMapper(field);
            if (fieldMapper == null) {
                throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + field + "]");
            }
            if (!(fieldMapper instanceof NumberFieldMapper)) {
                throw new FacetPhaseExecutionException(facetName, "field [" + field + "] isn't a number field, but a " + fieldMapper.fieldDataType().getType());
            }
            IndexNumericFieldData indexFieldData = context.fieldData().getForField(fieldMapper);
            return new StatisticalFacetExecutor(indexFieldData, context);
        } else {
            return new ScriptStatisticalFacetExecutor(scriptLang, script, scriptType, params, context);
        }
    }
}
