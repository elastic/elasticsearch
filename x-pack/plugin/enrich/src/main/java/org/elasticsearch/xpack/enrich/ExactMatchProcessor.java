/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.EnrichProcessorFactory.EnrichSpecification;

import java.util.List;
import java.util.Map;

final class ExactMatchProcessor extends AbstractProcessor {

    static final String ENRICH_KEY_FIELD_NAME = "enrich_key_field";

    private final CheckedFunction<SearchRequest, SearchResponse, Exception> searchRunner;

    private final String policyName;
    private final String enrichKey;
    private final boolean ignoreMissing;
    private final List<EnrichSpecification> specifications;

    ExactMatchProcessor(String tag,
                        Client client,
                        String policyName,
                        String enrichKey,
                        boolean ignoreMissing,
                        List<EnrichSpecification> specifications) {
        this(tag, (req) -> client.search(req).actionGet(), policyName, enrichKey, ignoreMissing, specifications);
    }

    ExactMatchProcessor(String tag,
                        CheckedFunction<SearchRequest, SearchResponse, Exception> searchRunner,
                        String policyName,
                        String enrichKey,
                        boolean ignoreMissing,
                        List<EnrichSpecification> specifications) {
        super(tag);
        this.searchRunner = searchRunner;
        this.policyName = policyName;
        this.enrichKey = enrichKey;
        this.ignoreMissing = ignoreMissing;
        this.specifications = specifications;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        // If a document does not have the enrich key, return the unchanged document
        final String value = ingestDocument.getFieldValue(enrichKey, String.class, ignoreMissing);
        if (value == null) {
            return ingestDocument;
        }

        TermQueryBuilder termQuery = new TermQueryBuilder(enrichKey, value);
        ConstantScoreQueryBuilder constantScore = new ConstantScoreQueryBuilder(termQuery);
        // TODO: Use a custom transport action instead of the search API
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.size(1);
        searchBuilder.trackScores(false);
        searchBuilder.fetchSource(specifications.stream().map(s -> s.sourceField).toArray(String[]::new), null);
        searchBuilder.query(constantScore);

        SearchRequest req = new SearchRequest();
        req.indices(EnrichPolicy.getBaseName(policyName));
        req.preference(Preference.LOCAL.type());
        req.source(searchBuilder);

        // TODO: Make this Async
        SearchResponse searchResponse = searchRunner.apply(req);

        // If the index is empty, return the unchanged document
        // If the enrich key does not exist in the index, throw an error
        // If no documents match the key, return the unchanged document
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        if (searchHits.length < 1) {
            return ingestDocument;
        } else if (searchHits.length > 1) {
            throw new IllegalStateException("more than one doc id matching for [" + enrichKey + "]");
        }

        // If a document is returned, add its fields to the document
        Map<String, Object> enrichDocument = searchHits[0].getSourceAsMap();
        assert enrichDocument != null : "enrich document for id [" + enrichKey + "] was empty despite non-zero search hits length";
        for (EnrichSpecification specification : specifications) {
            Object enrichFieldValue = enrichDocument.get(specification.sourceField);
            ingestDocument.setFieldValue(specification.targetField, enrichFieldValue);
        }
        return ingestDocument;
    }

    @Override
    public String getType() {
        return EnrichProcessorFactory.TYPE;
    }

    String getPolicyName() {
        return policyName;
    }

    String getEnrichKey() {
        return enrichKey;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    List<EnrichSpecification> getSpecifications() {
        return specifications;
    }
}
