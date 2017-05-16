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
package org.elasticsearch.legacy.search.aggregations.metrics.tophits;

import org.elasticsearch.legacy.ExceptionsHelper;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.xcontent.XContentParser;
import org.elasticsearch.legacy.search.SearchParseException;
import org.elasticsearch.legacy.search.aggregations.Aggregator;
import org.elasticsearch.legacy.search.aggregations.AggregatorFactory;
import org.elasticsearch.legacy.search.fetch.FetchPhase;
import org.elasticsearch.legacy.search.fetch.fielddata.FieldDataFieldsParseElement;
import org.elasticsearch.legacy.search.fetch.script.ScriptFieldsParseElement;
import org.elasticsearch.legacy.search.fetch.source.FetchSourceParseElement;
import org.elasticsearch.legacy.search.highlight.HighlighterParseElement;
import org.elasticsearch.legacy.search.internal.SearchContext;
import org.elasticsearch.legacy.search.sort.SortParseElement;

import java.io.IOException;

/**
 *
 */
public class TopHitsParser implements Aggregator.Parser {

    private final FetchPhase fetchPhase;
    private final SortParseElement sortParseElement;
    private final FetchSourceParseElement sourceParseElement;
    private final HighlighterParseElement highlighterParseElement;
    private final FieldDataFieldsParseElement fieldDataFieldsParseElement;
    private final ScriptFieldsParseElement scriptFieldsParseElement;

    @Inject
    public TopHitsParser(FetchPhase fetchPhase, SortParseElement sortParseElement, FetchSourceParseElement sourceParseElement, HighlighterParseElement highlighterParseElement, FieldDataFieldsParseElement fieldDataFieldsParseElement, ScriptFieldsParseElement scriptFieldsParseElement) {
        this.fetchPhase = fetchPhase;
        this.sortParseElement = sortParseElement;
        this.sourceParseElement = sourceParseElement;
        this.highlighterParseElement = highlighterParseElement;
        this.fieldDataFieldsParseElement = fieldDataFieldsParseElement;
        this.scriptFieldsParseElement = scriptFieldsParseElement;
    }

    @Override
    public String type() {
        return InternalTopHits.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
        TopHitsContext topHitsContext = new TopHitsContext(context);
        XContentParser.Token token;
        String currentFieldName = null;
        try {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("sort".equals(currentFieldName)) {
                    sortParseElement.parse(parser, topHitsContext);
                } else if ("_source".equals(currentFieldName)) {
                    sourceParseElement.parse(parser, topHitsContext);
                } else if (token.isValue()) {
                    switch (currentFieldName) {
                        case "from":
                            topHitsContext.from(parser.intValue());
                            break;
                        case "size":
                            topHitsContext.size(parser.intValue());
                            break;
                        case "track_scores":
                        case "trackScores":
                            topHitsContext.trackScores(parser.booleanValue());
                            break;
                        case "version":
                            topHitsContext.version(parser.booleanValue());
                            break;
                        case "explain":
                            topHitsContext.explain(parser.booleanValue());
                            break;
                        default:
                            throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    switch (currentFieldName) {
                        case "highlight":
                            highlighterParseElement.parse(parser, topHitsContext);
                            break;
                        case "scriptFields":
                        case "script_fields":
                            scriptFieldsParseElement.parse(parser, topHitsContext);
                            break;
                        default:
                            throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    switch (currentFieldName) {
                        case "fielddataFields":
                        case "fielddata_fields":
                            fieldDataFieldsParseElement.parse(parser, topHitsContext);
                            break;
                        default:
                            throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                    }
                } else {
                    throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
                }
            }
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
        return new TopHitsAggregator.Factory(aggregationName, fetchPhase, topHitsContext);
    }

}
