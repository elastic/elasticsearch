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

package org.elasticsearch.index.query.support;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsParseElement;
import org.elasticsearch.search.fetch.script.ScriptFieldsParseElement;
import org.elasticsearch.search.fetch.source.FetchSourceParseElement;
import org.elasticsearch.search.highlight.HighlighterParseElement;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.sort.SortParseElement;

import java.io.IOException;

public class InnerHitsQueryParserHelper {

    private final SortParseElement sortParseElement;
    private final FetchSourceParseElement sourceParseElement;
    private final HighlighterParseElement highlighterParseElement;
    private final ScriptFieldsParseElement scriptFieldsParseElement;
    private final FieldDataFieldsParseElement fieldDataFieldsParseElement;

    @Inject
    public InnerHitsQueryParserHelper(SortParseElement sortParseElement, FetchSourceParseElement sourceParseElement, HighlighterParseElement highlighterParseElement, ScriptFieldsParseElement scriptFieldsParseElement, FieldDataFieldsParseElement fieldDataFieldsParseElement) {
        this.sortParseElement = sortParseElement;
        this.sourceParseElement = sourceParseElement;
        this.highlighterParseElement = highlighterParseElement;
        this.scriptFieldsParseElement = scriptFieldsParseElement;
        this.fieldDataFieldsParseElement = fieldDataFieldsParseElement;
    }

    public Tuple<String, SubSearchContext> parse(QueryParseContext parserContext) throws IOException, QueryParsingException {
        String fieldName = null;
        XContentParser.Token token;
        String innerHitName = null;
        SubSearchContext subSearchContext = new SubSearchContext(SearchContext.current());
        try {
            XContentParser parser = parserContext.parser();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("name".equals(fieldName)) {
                        innerHitName = parser.textOrNull();
                    } else {
                        parseCommonInnerHitOptions(parser, token, fieldName, subSearchContext, sortParseElement, sourceParseElement, highlighterParseElement, scriptFieldsParseElement, fieldDataFieldsParseElement);
                    }
                } else {
                    parseCommonInnerHitOptions(parser, token, fieldName, subSearchContext, sortParseElement, sourceParseElement, highlighterParseElement, scriptFieldsParseElement, fieldDataFieldsParseElement);
                }
            }
        } catch (Exception e) {
            throw new QueryParsingException(parserContext.index(), "Failed to parse [_inner_hits]", e);
        }
        return new Tuple<>(innerHitName, subSearchContext);
    }

    public static void parseCommonInnerHitOptions(XContentParser parser, XContentParser.Token token, String fieldName, SubSearchContext subSearchContext,
                                                  SortParseElement sortParseElement, FetchSourceParseElement sourceParseElement, HighlighterParseElement highlighterParseElement,
                                                  ScriptFieldsParseElement scriptFieldsParseElement, FieldDataFieldsParseElement fieldDataFieldsParseElement) throws Exception {
        if ("sort".equals(fieldName)) {
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
                default:
                    throw new ElasticsearchIllegalArgumentException("Unknown key for a " + token + " for nested query: [" + fieldName + "].");
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            switch (fieldName) {
                case "fielddataFields":
                case "fielddata_fields":
                    fieldDataFieldsParseElement.parse(parser, subSearchContext);
                    break;
                case "fields":
                    boolean added = false;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String name = parser.text();
                        added = true;
                        subSearchContext.fieldNames().add(name);
                    }
                    if (!added) {
                        subSearchContext.emptyFieldNames();
                    }
                    break;
                default:
                    throw new ElasticsearchIllegalArgumentException("Unknown key for a " + token + " for nested query: [" + fieldName + "].");
            }
        } else if (token.isValue()) {
            switch (fieldName) {
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
                case "fields":
                    subSearchContext.fieldNames().add(parser.text());
                    break;
                default:
                    throw new ElasticsearchIllegalArgumentException("Unknown key for a " + token + " for nested query: [" + fieldName + "].");
            }
        }
    }
}
