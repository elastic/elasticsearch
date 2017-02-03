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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Classes implementing this interface provide a means to compute the quality of a result list
 * returned by some search.
 *
 * RelevancyLevel specifies the type of object determining the relevancy level of some known docid.
 * */
public interface RankedListQualityMetric extends ToXContent, NamedWriteable {

    /**
     * Returns a single metric representing the ranking quality of a set of returned documents
     * wrt. to a set of document Ids labeled as relevant for this search.
     *
     * @param taskId the id of the query for which the ranking is currently evaluated
     * @param hits the result hits as returned by a search request
     * @param ratedDocs the documents that were ranked by human annotators for this query case
     * @return some metric representing the quality of the result hit list wrt. to relevant doc ids.
     * */
    EvalQueryQuality evaluate(String taskId, SearchHit[] hits, List<RatedDocument> ratedDocs);

    static RankedListQualityMetric fromXContent(XContentParser parser) throws IOException {
        RankedListQualityMetric rc;
        Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[_na] missing required metric name");
        }
        String metricName = parser.currentName();

        // TODO maybe switch to using a plugable registry later?
        switch (metricName) {
        case Precision.NAME:
            rc = Precision.fromXContent(parser);
            break;
        case ReciprocalRank.NAME:
            rc = ReciprocalRank.fromXContent(parser);
            break;
        case DiscountedCumulativeGain.NAME:
            rc = DiscountedCumulativeGain.fromXContent(parser);
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

    static List<RatedSearchHit> joinHitsWithRatings(SearchHit[] hits, List<RatedDocument> ratedDocs) {
     // join hits with rated documents
        Map<DocumentKey, RatedDocument> ratedDocumentMap = ratedDocs.stream()
                .collect(Collectors.toMap(RatedDocument::getKey, item -> item));
        List<RatedSearchHit> ratedSearchHits = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            DocumentKey key = new DocumentKey(hit.getIndex(), hit.getType(), hit.getId());
            RatedDocument ratedDoc = ratedDocumentMap.get(key);
            if (ratedDoc != null) {
                ratedSearchHits.add(new RatedSearchHit(hit, Optional.of(ratedDoc.getRating())));
            } else {
                ratedSearchHits.add(new RatedSearchHit(hit, Optional.empty()));
            }
        }
        return ratedSearchHits;
    }

    static List<DocumentKey> filterUnknownDocuments(List<RatedSearchHit> ratedHits) {
        // join hits with rated documents
        List<DocumentKey> unknownDocs = ratedHits.stream()
                .filter(hit -> hit.getRating().isPresent() == false)
                .map(hit -> new DocumentKey(hit.getSearchHit().getIndex(), hit.getSearchHit().getType(), hit.getSearchHit().getId()))
                .collect(Collectors.toList());
           return unknownDocs;
       }

    default double combine(Collection<EvalQueryQuality> partialResults) {
        return partialResults.stream().mapToDouble(EvalQueryQuality::getQualityLevel).sum() / partialResults.size();
    }
}
