/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public abstract class AbstractEnrichProcessor extends AbstractProcessor {

    private final String policyName;
    private final EnrichProcessorFactory.SearchRunner searchRunner;
    private final TemplateScript.Factory field;
    private final TemplateScript.Factory targetField;
    private final boolean ignoreMissing;
    private final boolean overrideEnabled;
    protected final String matchField;
    protected final int maxMatches;
    private final String indexAlias;

    protected AbstractEnrichProcessor(
        String tag,
        String description,
        EnrichProcessorFactory.SearchRunner searchRunner,
        String policyName,
        TemplateScript.Factory field,
        TemplateScript.Factory targetField,
        boolean ignoreMissing,
        boolean overrideEnabled,
        String matchField,
        int maxMatches
    ) {
        super(tag, description);
        this.policyName = policyName;
        this.searchRunner = searchRunner;
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.overrideEnabled = overrideEnabled;
        this.matchField = matchField;
        this.maxMatches = maxMatches;
        // note: since the policyName determines the indexAlias, we can calculate this once
        this.indexAlias = EnrichPolicy.getBaseName(policyName);
    }

    public abstract QueryBuilder getQueryBuilder(Object fieldValue);

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        try {
            // If a document does not have the enrich key, return the unchanged document
            String renderedField = ingestDocument.renderTemplate(this.field);
            final Object value = ingestDocument.getFieldValue(renderedField, Object.class, ignoreMissing);
            if (value == null) {
                handler.accept(ingestDocument, null);
                return;
            }

            Supplier<SearchRequest> searchRequestSupplier = () -> {
                QueryBuilder queryBuilder = getQueryBuilder(value);
                ConstantScoreQueryBuilder constantScore = new ConstantScoreQueryBuilder(queryBuilder);
                SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
                searchBuilder.from(0);
                searchBuilder.size(maxMatches);
                searchBuilder.trackScores(false);
                searchBuilder.fetchSource(true);
                searchBuilder.query(constantScore);
                SearchRequest req = new SearchRequest();
                req.indices(indexAlias);
                req.preference(Preference.LOCAL.type());
                req.source(searchBuilder);
                return req;
            };

            searchRunner.accept(value, maxMatches, searchRequestSupplier, (searchHits, e) -> {
                if (e != null) {
                    handler.accept(null, e);
                    return;
                }

                // If the index is empty, return the unchanged document
                // If the enrich key does not exist in the index, throw an error
                // If no documents match the key, return the unchanged document
                if (searchHits.isEmpty()) {
                    handler.accept(ingestDocument, null);
                    return;
                }

                String renderedTargetField = ingestDocument.renderTemplate(this.targetField);
                if (overrideEnabled || ingestDocument.hasField(renderedTargetField) == false) {
                    if (maxMatches == 1) {
                        Map<?, ?> firstDocument = searchHits.get(0);
                        ingestDocument.setFieldValue(renderedTargetField, firstDocument);
                    } else {
                        List<Map<?, ?>> enrichDocuments = new ArrayList<>(searchHits.size());
                        enrichDocuments.addAll(searchHits);
                        ingestDocument.setFieldValue(renderedTargetField, enrichDocuments);
                    }
                }
                handler.accept(ingestDocument, null);
            });
        } catch (Exception e) {
            handler.accept(null, e);
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        throw new UnsupportedOperationException("this method should not get executed");
    }

    @Override
    public boolean isAsync() {
        return true;
    }

    public String getPolicyName() {
        return policyName;
    }

    @Override
    public String getType() {
        return EnrichProcessorFactory.TYPE;
    }

    String getField() {
        // used for testing only:
        return field.newInstance(Map.of()).execute();
    }

    String getTargetField() {
        // used for testing only:
        return targetField.newInstance(Map.of()).execute();
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    boolean isOverrideEnabled() {
        return overrideEnabled;
    }

    public String getMatchField() {
        return matchField;
    }

    int getMaxMatches() {
        return maxMatches;
    }

}
