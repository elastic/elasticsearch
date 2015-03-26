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
import org.elasticsearch.index.query.QueryParseContext;
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

import static org.elasticsearch.index.query.support.InnerHitsQueryParserHelper.parseCommonInnerHitOptions;

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
    public void parse(XContentParser parser, SearchContext searchContext) throws Exception {
        QueryParseContext parseContext = searchContext.queryParserService().getParseContext();
        parseContext.reset(parser);
        Map<String, InnerHitsContext.BaseInnerHits> innerHitsMap = parseInnerHits(parser, parseContext, searchContext);
        if (innerHitsMap != null) {
            searchContext.innerHits(new InnerHitsContext(innerHitsMap));
        }
    }

    private Map<String, InnerHitsContext.BaseInnerHits> parseInnerHits(XContentParser parser, QueryParseContext parseContext, SearchContext searchContext) throws Exception {
        XContentParser.Token token;
        Map<String, InnerHitsContext.BaseInnerHits> innerHitsMap = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new ElasticsearchIllegalArgumentException("Unexpected token " + token + " in [inner_hits]: inner_hit definitions must start with the name of the inner_hit.");
            }
            final String innerHitName = parser.currentName();
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchIllegalArgumentException("Inner hit definition for [" + innerHitName + " starts with a [" + token + "], expected a [" + XContentParser.Token.START_OBJECT + "].");
            }
            InnerHitsContext.BaseInnerHits innerHits = parseInnerHit(parser, parseContext, searchContext, innerHitName);
            if (innerHitsMap == null) {
                innerHitsMap = new HashMap<>();
            }
            innerHitsMap.put(innerHitName, innerHits);
        }
        return innerHitsMap;
    }

    private InnerHitsContext.BaseInnerHits parseInnerHit(XContentParser parser, QueryParseContext parseContext, SearchContext searchContext, String innerHitName) throws Exception {
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ElasticsearchIllegalArgumentException("Unexpected token " + token + " inside inner hit definition. Either specify [path] or [type] object");
        }
        String fieldName = parser.currentName();
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchIllegalArgumentException("Inner hit definition for [" + innerHitName + " starts with a [" + token + "], expected a [" + XContentParser.Token.START_OBJECT + "].");
        }

        String nestedPath = null;
        String type = null;
        switch (fieldName) {
            case "path":
                nestedPath = parser.currentName();
                break;
            case "type":
                type = parser.currentName();
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

        final InnerHitsContext.BaseInnerHits innerHits;
        if (nestedPath != null) {
            innerHits = parseNested(parser, parseContext, searchContext, fieldName);
        } else if (type != null) {
            innerHits = parseParentChild(parser, parseContext, searchContext, fieldName);
        } else {
            throw new ElasticsearchIllegalArgumentException("Either [path] or [type] must be defined");
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

        return innerHits;
    }

    private InnerHitsContext.ParentChildInnerHits parseParentChild(XContentParser parser, QueryParseContext parseContext, SearchContext searchContext, String type) throws Exception {
        ParseResult parseResult = parseSubSearchContext(searchContext, parseContext, parser);
        DocumentMapper documentMapper = searchContext.mapperService().documentMapper(type);
        if (documentMapper == null) {
            throw new ElasticsearchIllegalArgumentException("type [" + type + "] doesn't exist");
        }
        return new InnerHitsContext.ParentChildInnerHits(parseResult.context(), parseResult.query(), parseResult.childInnerHits(), documentMapper);
    }

    private InnerHitsContext.NestedInnerHits parseNested(XContentParser parser, QueryParseContext parseContext, SearchContext searchContext, String nestedPath) throws Exception {
        MapperService.SmartNameObjectMapper smartNameObjectMapper = searchContext.smartNameObjectMapper(nestedPath);
        if (smartNameObjectMapper == null || !smartNameObjectMapper.hasMapper()) {
            throw new ElasticsearchIllegalArgumentException("path [" + nestedPath +"] doesn't exist");
        }
        ObjectMapper childObjectMapper = smartNameObjectMapper.mapper();
        if (!childObjectMapper.nested().isNested()) {
            throw new ElasticsearchIllegalArgumentException("path [" + nestedPath +"] isn't nested");
        }
        ObjectMapper parentObjectMapper = parseContext.nestedScope().nextLevel(childObjectMapper);
        ParseResult parseResult = parseSubSearchContext(searchContext, parseContext, parser);
        parseContext.nestedScope().previousLevel();

        return new InnerHitsContext.NestedInnerHits(parseResult.context(), parseResult.query(), parseResult.childInnerHits(), parentObjectMapper, childObjectMapper);
    }

    private ParseResult parseSubSearchContext(SearchContext searchContext, QueryParseContext parseContext, XContentParser parser) throws Exception {
        Query query = null;
        Map<String, InnerHitsContext.BaseInnerHits> childInnerHits = null;
        SubSearchContext subSearchContext = new SubSearchContext(searchContext);
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(fieldName)) {
                    query = searchContext.queryParserService().parseInnerQuery(parseContext);
                } else if ("inner_hits".equals(fieldName)) {
                    childInnerHits = parseInnerHits(parser, parseContext, searchContext);
                } else {
                    parseCommonInnerHitOptions(parser, token, fieldName, subSearchContext, sortParseElement, sourceParseElement, highlighterParseElement, scriptFieldsParseElement, fieldDataFieldsParseElement);
                }
            } else {
                parseCommonInnerHitOptions(parser, token, fieldName, subSearchContext, sortParseElement, sourceParseElement, highlighterParseElement, scriptFieldsParseElement, fieldDataFieldsParseElement);
            }
        }

        if (query == null) {
            query = new MatchAllDocsQuery();
        }
        return new ParseResult(subSearchContext, query, childInnerHits);
    }

    private static final class ParseResult {

        private final SubSearchContext context;
        private final Query query;
        private final Map<String, InnerHitsContext.BaseInnerHits> childInnerHits;

        private ParseResult(SubSearchContext context, Query query, Map<String, InnerHitsContext.BaseInnerHits> childInnerHits) {
            this.context = context;
            this.query = query;
            this.childInnerHits = childInnerHits;
        }

        public SubSearchContext context() {
            return context;
        }

        public Query query() {
            return query;
        }

        public Map<String, InnerHitsContext.BaseInnerHits> childInnerHits() {
            return childInnerHits;
        }
    }
}
