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

package org.elasticsearch.search.fetch.innerhits;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.NestedQueryParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsParseElement;
import org.elasticsearch.search.fetch.script.ScriptFieldsParseElement;
import org.elasticsearch.search.fetch.source.FetchSourceParseElement;
import org.elasticsearch.search.highlight.HighlighterParseElement;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.sort.SortParseElement;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class InnerHitsParseElement implements SearchParseElement {

    private final SortParseElement sortParseElement;
    private final FetchSourceParseElement sourceParseElement;
    private final HighlighterParseElement highlighterParseElement;
    private final FieldDataFieldsParseElement fieldDataFieldsParseElement;
    private final ScriptFieldsParseElement scriptFieldsParseElement;

    public InnerHitsParseElement(SortParseElement sortParseElement, FetchSourceParseElement sourceParseElement, HighlighterParseElement highlighterParseElement, FieldDataFieldsParseElement fieldDataFieldsParseElement, ScriptFieldsParseElement scriptFieldsParseElement) {
        this.sortParseElement = sortParseElement;
        this.sourceParseElement = sourceParseElement;
        this.highlighterParseElement = highlighterParseElement;
        this.fieldDataFieldsParseElement = fieldDataFieldsParseElement;
        this.scriptFieldsParseElement = scriptFieldsParseElement;
    }

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        Map<String, InnerHitsContext.BaseInnerHits> innerHitsMap = parseInnerHits(parser, context);
        if (innerHitsMap != null) {
            context.innerHits(new InnerHitsContext(innerHitsMap));
        }
    }

    private Map<String, InnerHitsContext.BaseInnerHits> parseInnerHits(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token;
        Map<String, InnerHitsContext.BaseInnerHits> innerHitsMap = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new ElasticsearchIllegalArgumentException("Unexpected token " + token + " in [inner_hits]: aggregations definitions must start with the name of the aggregation.");
            }
            final String innerHitName = parser.currentName();
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchIllegalArgumentException("Inner hit definition for [" + innerHitName + " starts with a [" + token + "], expected a [" + XContentParser.Token.START_OBJECT + "].");
            }
            InnerHitsContext.BaseInnerHits innerHits = parseInnerHit(parser, context, innerHitName);
            if (innerHitsMap == null) {
                innerHitsMap = new HashMap<>();
            }
            innerHitsMap.put(innerHitName, innerHits);
        }
        return innerHitsMap;
    }

    private InnerHitsContext.BaseInnerHits parseInnerHit(XContentParser parser, SearchContext context, String innerHitName) throws Exception {
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ElasticsearchIllegalArgumentException("Unexpected token " + token + " inside inner hit definition. Either specify [path] or [type] object");
        }
        String fieldName = parser.currentName();
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchIllegalArgumentException("Inner hit definition for [" + innerHitName + " starts with a [" + token + "], expected a [" + XContentParser.Token.START_OBJECT + "].");
        }
        final boolean nested;
        switch (fieldName) {
            case "path":
                nested = true;
                break;
            case "type":
                nested = false;
                break;
            default:
                throw new ElasticsearchIllegalArgumentException("Either path or type object must be defined");
        }
        token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ElasticsearchIllegalArgumentException("Unexpected token " + token + " inside inner hit definition. Either specify [path] or [type] object");
        }
        fieldName = parser.currentName();
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchIllegalArgumentException("Inner hit definition for [" + innerHitName + " starts with a [" + token + "], expected a [" + XContentParser.Token.START_OBJECT + "].");
        }

        NestedQueryParser.LateBindingParentFilter parentFilter = null;
        NestedQueryParser.LateBindingParentFilter currentFilter = null;


        String nestedPath = null;
        String type = null;
        if (nested) {
            nestedPath = fieldName;
            currentFilter = new NestedQueryParser.LateBindingParentFilter();
            parentFilter = NestedQueryParser.parentFilterContext.get();
            NestedQueryParser.parentFilterContext.set(currentFilter);
        } else {
            type = fieldName;
        }

        Query query = null;
        Map<String, InnerHitsContext.BaseInnerHits> childInnerHits = null;
        SubSearchContext subSearchContext = new SubSearchContext(context);
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if ("sort".equals(fieldName)) {
                sortParseElement.parse(parser, subSearchContext);
            } else if ("_source".equals(fieldName)) {
                sourceParseElement.parse(parser, subSearchContext);
            } else if (token == XContentParser.Token.START_OBJECT) {
                switch (fieldName) {
                    case "highlight":
                        highlighterParseElement.parse(parser, subSearchContext);
                        break;
                    case "scriptFields":
                    case "script_fields":
                        scriptFieldsParseElement.parse(parser, subSearchContext);
                        break;
                    case "inner_hits":
                        childInnerHits = parseInnerHits(parser, subSearchContext);
                        break;
                    case "query":
                        query = context.queryParserService().parse(parser).query();
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unknown key for a " + token + " in [" + innerHitName + "]: [" + fieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                switch (fieldName) {
                    case "fielddataFields":
                    case "fielddata_fields":
                        fieldDataFieldsParseElement.parse(parser, subSearchContext);
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unknown key for a " + token + " in [" + innerHitName + "]: [" + fieldName + "].");
                }
            } else if (token.isValue()) {
                switch (fieldName) {
                    case "query" :
                        query = context.queryParserService().parse(parser).query();
                        break;
                    case "from":
                        subSearchContext.from(parser.intValue());
                        break;
                    case "size":
                        subSearchContext.size(parser.intValue());
                        break;
                    case "track_scores":
                    case "trackScores":
                        subSearchContext.trackScores(parser.booleanValue());
                        break;
                    case "version":
                        subSearchContext.version(parser.booleanValue());
                        break;
                    case "explain":
                        subSearchContext.explain(parser.booleanValue());
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unknown key for a " + token + " in [" + innerHitName + "]: [" + fieldName + "].");
                }
            } else {
                throw new ElasticsearchIllegalArgumentException("Unexpected token " + token + " in [" + innerHitName + "].");
            }
        }

        // Completely consume all json objects:
        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchIllegalArgumentException("Expected [" + XContentParser.Token.END_OBJECT + "] token, but got a [" + token + "] token.");
        }
        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchIllegalArgumentException("Expected [" + XContentParser.Token.END_OBJECT + "] token, but got a [" + token + "] token.");
        }

        if (query == null) {
            query = new MatchAllDocsQuery();
        }

        if (nestedPath != null && type != null) {
            throw new ElasticsearchIllegalArgumentException("Either [path] or [type] can be defined not both");
        } else if (nestedPath != null) {
            MapperService.SmartNameObjectMapper smartNameObjectMapper = context.smartNameObjectMapper(nestedPath);
            if (smartNameObjectMapper == null || !smartNameObjectMapper.hasMapper()) {
                throw new ElasticsearchIllegalArgumentException("path [" + nestedPath +"] doesn't exist");
            }
            ObjectMapper childObjectMapper = smartNameObjectMapper.mapper();
            if (!childObjectMapper.nested().isNested()) {
                throw new ElasticsearchIllegalArgumentException("path [" + nestedPath +"] isn't nested");
            }
            DocumentMapper childDocumentMapper = smartNameObjectMapper.docMapper();
            if (childDocumentMapper == null) {
                for (DocumentMapper documentMapper : context.mapperService().docMappers(false)) {
                    if (documentMapper.objectMappers().containsKey(nestedPath)) {
                        childDocumentMapper = documentMapper;
                        break;
                    }
                }
            }
            if (currentFilter != null && childDocumentMapper != null) {
                currentFilter.filter = context.bitsetFilterCache().getBitDocIdSetFilter(childObjectMapper.nestedTypeFilter());
                NestedQueryParser.parentFilterContext.set(parentFilter);
            }

            ObjectMapper parentObjectMapper = childDocumentMapper.findParentObjectMapper(childObjectMapper);
            return new InnerHitsContext.NestedInnerHits(subSearchContext, query, childInnerHits, parentObjectMapper, childObjectMapper);
        } else if (type != null) {
            DocumentMapper documentMapper = context.mapperService().documentMapper(type);
            if (documentMapper == null) {
                throw new ElasticsearchIllegalArgumentException("type [" + type + "] doesn't exist");
            }
            return new InnerHitsContext.ParentChildInnerHits(subSearchContext, query, childInnerHits, documentMapper);
        } else {
            throw new ElasticsearchIllegalArgumentException("Either [path] or [type] must be defined");
        }
    }
}
