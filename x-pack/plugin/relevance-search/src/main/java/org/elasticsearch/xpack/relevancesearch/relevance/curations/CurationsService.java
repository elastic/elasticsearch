/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance.curations;

import org.elasticsearch.client.internal.Client;

import java.util.List;
import java.util.Map;

/**
 * Manage relevance settings, retrieving and updating the corresponding documents in .ent-search index
 */
public class CurationsService {

    public static final String ENT_SEARCH_INDEX = ".ent-search";
    public static final String CURATIONS_SETTINGS_PREFIX = "curations-";
    private static final String CONDITIONS_FIELD = "conditions";
    private static final String PINNED_DOCS_FIELD = "pinned_document_ids";
    private static final String EXCLUDED_DOCS_FIELD = "excluded_document_ids";
    private static final String CONTEXT_ATTR = "context";
    private static final String VALUE_ATTR = "value";
    private static final String ID_ATTR = "_id";
    private static final String INDEX_ATTR = "_index";

    private final Client client;

    public CurationsService(final Client client) {
        this.client = client;
    }

    public CurationSettings getCurationsSettings(String curationId) throws CurationsSettingsNotFoundException {
        // TODO cache relevance settings, including cache invalidation
        final Map<String, Object> settingsContent = client.prepareGet(ENT_SEARCH_INDEX, CURATIONS_SETTINGS_PREFIX + curationId)
            .get()
            .getSource();

        if (settingsContent == null) {
            throw new CurationsSettingsNotFoundException("Curation settings " + curationId + " not found");
        }

        return parseCurationSettings(settingsContent);
    }

    private CurationSettings parseCurationSettings(Map<String, Object> source) throws IllegalArgumentException {
        // TODO Probably worth to take a look into document mappers in case they can be used for parsing
        // see org/elasticsearch/index/mapper/DocumentParser.java

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> sourceConditions = (List<Map<String, Object>>) source.get(CONDITIONS_FIELD);
        List<Condition> conditions = sourceConditions.stream().map(this::parseCondition).toList();

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> sourcePinnedDocs = (List<Map<String, Object>>) source.get(PINNED_DOCS_FIELD);
        List<CurationSettings.DocumentReference> pinnedDocs = sourcePinnedDocs.stream().map(this::parseDocumentReference).toList();

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> sourceExcludedDocs = (List<Map<String, Object>>) source.get(EXCLUDED_DOCS_FIELD);
        List<CurationSettings.DocumentReference> excludedDocs = sourceExcludedDocs.stream().map(this::parseDocumentReference).toList();

        return new CurationSettings(pinnedDocs, excludedDocs, conditions);
    }

    private CurationSettings.DocumentReference parseDocumentReference(Map<String, Object> sourceDoc) {
        return new CurationSettings.DocumentReference((String) sourceDoc.get(ID_ATTR), (String) sourceDoc.get(INDEX_ATTR));
    }

    private Condition parseCondition(Map<String, Object> sourceCondition) throws IllegalArgumentException {
        return Condition.buildCondition((String) sourceCondition.get(CONTEXT_ATTR), (String) sourceCondition.get(VALUE_ATTR));
    }

    public static class CurationsSettingsNotFoundException extends Exception {
        public CurationsSettingsNotFoundException(String message) {
            super(message);
        }
    }
}
