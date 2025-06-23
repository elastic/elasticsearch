/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.index.rankeval.RatedDocument.DocumentKey;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xcontent.ToXContentObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.Collectors;

/**
 * Implementations of {@link EvaluationMetric} need to provide a way to compute the quality metric for
 * a result list returned by some search (@link {@link SearchHits}) and a list of rated documents.
 */
public interface EvaluationMetric extends ToXContentObject, NamedWriteable {

    /**
     * Evaluates a single ranking evaluation case.
     *
     * @param taskId
     *            an identifier of the query for which the search ranking is
     *            evaluated
     * @param hits
     *            the search result hits
     * @param ratedDocs
     *            the documents that contain the document rating for this query case
     * @return an {@link EvalQueryQuality} instance that contains the metric score
     *         with respect to the provided search hits and ratings
     */
    EvalQueryQuality evaluate(String taskId, SearchHit[] hits, List<RatedDocument> ratedDocs);

    /**
     * Joins hits with rated documents using the joint _index/_id document key.
     */
    static List<RatedSearchHit> joinHitsWithRatings(SearchHit[] hits, List<RatedDocument> ratedDocs) {
        Map<DocumentKey, RatedDocument> ratedDocumentMap = ratedDocs.stream()
            .collect(Collectors.toMap(RatedDocument::getKey, item -> item));
        List<RatedSearchHit> ratedSearchHits = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            DocumentKey key = new DocumentKey(hit.getIndex(), hit.getId());
            RatedDocument ratedDoc = ratedDocumentMap.get(key);
            if (ratedDoc != null) {
                ratedSearchHits.add(new RatedSearchHit(hit, OptionalInt.of(ratedDoc.getRating())));
            } else {
                ratedSearchHits.add(new RatedSearchHit(hit, OptionalInt.empty()));
            }
        }
        return ratedSearchHits;
    }

    /**
     * Filter {@link RatedSearchHit}s that do not have a rating.
     */
    static List<DocumentKey> filterUnratedDocuments(List<RatedSearchHit> ratedHits) {
        return ratedHits.stream()
            .filter(hit -> hit.getRating().isPresent() == false)
            .map(hit -> new DocumentKey(hit.getSearchHit().getIndex(), hit.getSearchHit().getId()))
            .collect(Collectors.toList());
    }

    /**
     * Combine several {@link EvalQueryQuality} results into the overall evaluation score.
     * This defaults to averaging over the partial results, but can be overwritten to obtain a different behavior.
     */
    default double combine(Collection<EvalQueryQuality> partialResults) {
        return partialResults.stream().mapToDouble(EvalQueryQuality::metricScore).sum() / partialResults.size();
    }

    /**
     * Metrics can define a size of the search hits windows they want to retrieve by overwriting
     * this method. The default implementation returns an empty optional.
     * @return the number of search hits this metrics requests
     */
    default OptionalInt forcedSearchSize() {
        return OptionalInt.empty();
    }
}
