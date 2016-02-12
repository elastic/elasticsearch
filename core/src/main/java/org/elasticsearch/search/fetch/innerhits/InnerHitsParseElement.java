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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryShardContext;
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
        QueryShardContext context = searchContext.getQueryShardContext();
        context.reset(parser);
        Map<String, InnerHitsContext.BaseInnerHits> topLevelInnerHits = parseInnerHits(parser, context, searchContext);
        if (topLevelInnerHits != null) {
            InnerHitsContext innerHitsContext = searchContext.innerHits();
            innerHitsContext.addInnerHitDefinitions(topLevelInnerHits);
        }
    }

    private Map<String, InnerHitsContext.BaseInnerHits> parseInnerHits(XContentParser parser, QueryShardContext context, SearchContext searchContext) throws Exception {
        XContentParser.Token token;
        Map<String, InnerHitsContext.BaseInnerHits> innerHitsMap = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalArgumentException("Unexpected token " + token + " in [inner_hits]: inner_hit definitions must start with the name of the inner_hit.");
            }
            final String innerHitName = parser.currentName();
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Inner hit definition for [" + innerHitName + " starts with a [" + token + "], expected a [" + XContentParser.Token.START_OBJECT + "].");
            }
            InnerHitsContext.BaseInnerHits innerHits = parseInnerHit(parser, context, searchContext, innerHitName);
            if (innerHitsMap == null) {
                innerHitsMap = new HashMap<>();
            }
            innerHitsMap.put(innerHitName, innerHits);
        }
        return innerHitsMap;
    }

    private InnerHitsContext.BaseInnerHits parseInnerHit(XContentParser parser, QueryShardContext context, SearchContext searchContext, String innerHitName) throws Exception {
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new IllegalArgumentException("Unexpected token " + token + " inside inner hit definition. Either specify [path] or [type] object");
        }
        String fieldName = parser.currentName();
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Inner hit definition for [" + innerHitName + " starts with a [" + token + "], expected a [" + XContentParser.Token.START_OBJECT + "].");
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
                throw new IllegalArgumentException("Either path or type object must be defined");
        }
        token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new IllegalArgumentException("Unexpected token " + token + " inside inner hit definition. Either specify [path] or [type] object");
        }
        fieldName = parser.currentName();
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Inner hit definition for [" + innerHitName + " starts with a [" + token + "], expected a [" + XContentParser.Token.START_OBJECT + "].");
        }

        final InnerHitsContext.BaseInnerHits innerHits;
        if (nestedPath != null) {
            innerHits = parseNested(parser, context, searchContext, fieldName);
        } else if (type != null) {
            innerHits = parseParentChild(parser, context, searchContext, fieldName);
        } else {
            throw new IllegalArgumentException("Either [path] or [type] must be defined");
        }

        // Completely consume all json objects:
        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new IllegalArgumentException("Expected [" + XContentParser.Token.END_OBJECT + "] token, but got a [" + token + "] token.");
        }
        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new IllegalArgumentException("Expected [" + XContentParser.Token.END_OBJECT + "] token, but got a [" + token + "] token.");
        }

        return innerHits;
    }

    private InnerHitsContext.ParentChildInnerHits parseParentChild(XContentParser parser, QueryShardContext context, SearchContext searchContext, String type) throws Exception {
        ParseResult parseResult = parseSubSearchContext(searchContext, context, parser);
        DocumentMapper documentMapper = searchContext.mapperService().documentMapper(type);
        if (documentMapper == null) {
            throw new IllegalArgumentException("type [" + type + "] doesn't exist");
        }
        return new InnerHitsContext.ParentChildInnerHits(parseResult.context(), parseResult.query(), parseResult.childInnerHits(), context.getMapperService(), documentMapper);
    }

    private InnerHitsContext.NestedInnerHits parseNested(XContentParser parser, QueryShardContext context, SearchContext searchContext, String nestedPath) throws Exception {
        ObjectMapper objectMapper = searchContext.getObjectMapper(nestedPath);
        if (objectMapper == null) {
            throw new IllegalArgumentException("path [" + nestedPath +"] doesn't exist");
        }
        if (objectMapper.nested().isNested() == false) {
            throw new IllegalArgumentException("path [" + nestedPath +"] isn't nested");
        }
        ObjectMapper parentObjectMapper = context.nestedScope().nextLevel(objectMapper);
        ParseResult parseResult = parseSubSearchContext(searchContext, context, parser);
        context.nestedScope().previousLevel();

        return new InnerHitsContext.NestedInnerHits(parseResult.context(), parseResult.query(), parseResult.childInnerHits(), parentObjectMapper, objectMapper);
    }

    private ParseResult parseSubSearchContext(SearchContext searchContext, QueryShardContext context, XContentParser parser) throws Exception {
        ParsedQuery query = null;
        Map<String, InnerHitsContext.BaseInnerHits> childInnerHits = null;
        SubSearchContext subSearchContext = new SubSearchContext(searchContext);
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(fieldName)) {
                    Query q = context.parseInnerQuery();
                    query = new ParsedQuery(q, context.copyNamedQueries());
                } else if ("inner_hits".equals(fieldName)) {
                    childInnerHits = parseInnerHits(parser, context, searchContext);
                } else {
                    parseCommonInnerHitOptions(parser, token, fieldName, subSearchContext, sortParseElement, sourceParseElement, highlighterParseElement, scriptFieldsParseElement, fieldDataFieldsParseElement);
                }
            } else {
                parseCommonInnerHitOptions(parser, token, fieldName, subSearchContext, sortParseElement, sourceParseElement, highlighterParseElement, scriptFieldsParseElement, fieldDataFieldsParseElement);
            }
        }

        if (query == null) {
            query = ParsedQuery.parsedMatchAllQuery();
        }
        return new ParseResult(subSearchContext, query, childInnerHits);
    }

    private static final class ParseResult {

        private final SubSearchContext context;
        private final ParsedQuery query;
        private final Map<String, InnerHitsContext.BaseInnerHits> childInnerHits;

        private ParseResult(SubSearchContext context, ParsedQuery query, Map<String, InnerHitsContext.BaseInnerHits> childInnerHits) {
            this.context = context;
            this.query = query;
            this.childInnerHits = childInnerHits;
        }

        public SubSearchContext context() {
            return context;
        }

        public ParsedQuery query() {
            return query;
        }

        public Map<String, InnerHitsContext.BaseInnerHits> childInnerHits() {
            return childInnerHits;
        }
    }
}
