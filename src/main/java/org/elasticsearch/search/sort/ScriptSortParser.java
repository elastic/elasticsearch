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

package org.elasticsearch.search.sort;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.SortField;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleScriptDataComparator;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.index.fielddata.fieldcomparator.StringScriptDataComparator;
import org.elasticsearch.index.mapper.ObjectMappers;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.search.nested.NestedFieldComparatorSource;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Map;

/**
 *
 */
public class ScriptSortParser implements SortParser {

    @Override
    public String[] names() {
        return new String[]{"_script"};
    }

    @Override
    public SortField parse(XContentParser parser, SearchContext context) throws Exception {
        String script = null;
        String scriptLang = null;
        String type = null;
        Map<String, Object> params = null;
        boolean reverse = false;
        MultiValueMode sortMode = null;
        String nestedPath = null;
        Filter nestedFilter = null;

        XContentParser.Token token;
        String currentName = parser.currentName();
        ScriptService.ScriptType scriptType = ScriptService.ScriptType.INLINE;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentName)) {
                    params = parser.map();
                } else if ("nested_filter".equals(currentName) || "nestedFilter".equals(currentName)) {
                    ParsedFilter parsedFilter = context.queryParserService().parseInnerFilter(parser);
                    nestedFilter = parsedFilter == null ? null : parsedFilter.filter();
                }
            } else if (token.isValue()) {
                if ("reverse".equals(currentName)) {
                    reverse = parser.booleanValue();
                } else if ("order".equals(currentName)) {
                    reverse = "desc".equals(parser.text());
                } else if (ScriptService.SCRIPT_INLINE.match(currentName)) {
                    script = parser.text();
                    scriptType = ScriptService.ScriptType.INLINE;
                } else if (ScriptService.SCRIPT_ID.match(currentName)) {
                    script = parser.text();
                    scriptType = ScriptService.ScriptType.INDEXED;
                } else if (ScriptService.SCRIPT_FILE.match(currentName)) {
                    script = parser.text();
                    scriptType = ScriptService.ScriptType.FILE;
                } else if (ScriptService.SCRIPT_LANG.match(currentName)) {
                    scriptLang = parser.text();
                } else if ("type".equals(currentName)) {
                    type = parser.text();
                } else if ("mode".equals(currentName)) {
                    sortMode = MultiValueMode.fromString(parser.text());
                } else if ("nested_path".equals(currentName) || "nestedPath".equals(currentName)) {
                    nestedPath = parser.text();
                }
            }
        }

        if (script == null) {
            throw new SearchParseException(context, "_script sorting requires setting the script to sort by");
        }
        if (type == null) {
            throw new SearchParseException(context, "_script sorting requires setting the type of the script");
        }
        SearchScript searchScript = context.scriptService().search(context.lookup(), scriptLang, script, scriptType, params);
        IndexFieldData.XFieldComparatorSource fieldComparatorSource;
        if ("string".equals(type)) {
            fieldComparatorSource = StringScriptDataComparator.comparatorSource(searchScript);
        } else if ("number".equals(type)) {
            fieldComparatorSource = DoubleScriptDataComparator.comparatorSource(searchScript);
        } else {
            throw new SearchParseException(context, "custom script sort type [" + type + "] not supported");
        }

        if ("string".equals(type) && (sortMode == MultiValueMode.SUM || sortMode == MultiValueMode.AVG)) {
            throw new SearchParseException(context, "type [string] doesn't support mode [" + sortMode + "]");
        }

        if (sortMode == null) {
            sortMode = reverse ? MultiValueMode.MAX : MultiValueMode.MIN;
        }

        // If nested_path is specified, then wrap the `fieldComparatorSource` in a `NestedFieldComparatorSource`
        ObjectMapper objectMapper;
        if (nestedPath != null) {
            ObjectMappers objectMappers = context.mapperService().objectMapper(nestedPath);
            if (objectMappers == null) {
                throw new ElasticsearchIllegalArgumentException("failed to find nested object mapping for explicit nested path [" + nestedPath + "]");
            }
            objectMapper = objectMappers.mapper();
            if (!objectMapper.nested().isNested()) {
                throw new ElasticsearchIllegalArgumentException("mapping for explicit nested path is not mapped as nested: [" + nestedPath + "]");
            }

            Filter rootDocumentsFilter = context.filterCache().cache(NonNestedDocsFilter.INSTANCE);
            Filter innerDocumentsFilter;
            if (nestedFilter != null) {
                innerDocumentsFilter = context.filterCache().cache(nestedFilter);
            } else {
                innerDocumentsFilter = context.filterCache().cache(objectMapper.nestedTypeFilter());
            }
            fieldComparatorSource = new NestedFieldComparatorSource(sortMode, fieldComparatorSource, rootDocumentsFilter, innerDocumentsFilter);
        }

        return new SortField("_script", fieldComparatorSource, reverse);
    }
}
