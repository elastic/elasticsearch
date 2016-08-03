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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.List;

/**
 * Classes implementing this interface provide a means to compute the quality of a result list
 * returned by some search.
 *
 * RelevancyLevel specifies the type of object determining the relevancy level of some known docid.
 * */
public abstract class RankedListQualityMetric implements NamedWriteable {

    /**
     * Returns a single metric representing the ranking quality of a set of returned documents
     * wrt. to a set of document Ids labeled as relevant for this search.
     *
     * @param hits the result hits as returned by some search
     * @return some metric representing the quality of the result hit list wrt. to relevant doc ids.
     * */
    public abstract EvalQueryQuality evaluate(SearchHit[] hits, List<RatedDocument> ratedDocs);

    public static RankedListQualityMetric fromXContent(XContentParser parser, ParseFieldMatcherSupplier context) throws IOException {
        RankedListQualityMetric rc;
        Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[_na] missing required metric name");
        }
        String metricName = parser.currentName();

        // TODO maybe switch to using a plugable registry later?
        switch (metricName) {
        case PrecisionAtN.NAME:
            rc = PrecisionAtN.fromXContent(parser, context);
            break;
        default:
            throw new ParsingException(parser.getTokenLocation(), "[_na] unknown query metric name [{}]", metricName);
        }
        if (parser.currentToken() == XContentParser.Token.END_OBJECT) {
            // if we are at END_OBJECT, move to the next one...
            parser.nextToken();
        }
        return rc;
    }
}
