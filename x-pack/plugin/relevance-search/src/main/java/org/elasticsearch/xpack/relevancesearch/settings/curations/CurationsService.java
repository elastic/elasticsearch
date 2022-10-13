/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.settings.curations;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xpack.relevancesearch.settings.AbstractSettingsService;
import org.elasticsearch.xpack.relevancesearch.settings.index.IndexCreationService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manage relevance settings, retrieving and updating the corresponding documents in .ent-search index
 */
public class CurationsService extends AbstractSettingsService<CurationsGroup> {

    private static final String CONDITIONS_FIELD = "conditions";
    private static final String PINNED_DOCS_FIELD = "pinned_document_ids";
    private static final String EXCLUDED_DOCS_FIELD = "excluded_document_ids";
    private static final String CONTEXT_ATTR = "context";
    private static final String VALUE_ATTR = "value";
    private static final String ID_ATTR = "_id";
    private static final String INDEX_ATTR = "_index";
    public static final String CURATIONS_GROUP_NAME = "group_name";

    public CurationsService(final Client client) {
        super(client);
    }

    private static CurationSettings parseCurationsSettings(Map<String, Object> source) throws IllegalArgumentException {
        // TODO Probably worth to take a look into document mappers in case they can be used for parsing
        // see org/elasticsearch/index/mapper/DocumentParser.java

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> sourceConditions = (List<Map<String, Object>>) source.getOrDefault(
            CONDITIONS_FIELD,
            Collections.emptyList()
        );
        List<Condition> conditions = sourceConditions.stream().map(CurationsService::parseCondition).toList();

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> sourcePinnedDocs = (List<Map<String, Object>>) source.getOrDefault(
            PINNED_DOCS_FIELD,
            Collections.emptyList()
        );
        List<CurationSettings.DocumentReference> pinnedDocs = sourcePinnedDocs.stream()
            .map(CurationsService::parseDocumentReference)
            .toList();

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> sourceExcludedDocs = (List<Map<String, Object>>) source.getOrDefault(
            EXCLUDED_DOCS_FIELD,
            Collections.emptyList()
        );
        List<CurationSettings.DocumentReference> excludedDocs = sourceExcludedDocs.stream()
            .map(CurationsService::parseDocumentReference)
            .toList();

        return new CurationSettings(pinnedDocs, excludedDocs, conditions);
    }

    private static CurationSettings.DocumentReference parseDocumentReference(Map<String, Object> sourceDoc) {
        return new CurationSettings.DocumentReference((String) sourceDoc.get(ID_ATTR), (String) sourceDoc.get(INDEX_ATTR));
    }

    private static Condition parseCondition(Map<String, Object> sourceCondition) throws IllegalArgumentException {
        return Condition.buildCondition((String) sourceCondition.get(CONTEXT_ATTR), (String) sourceCondition.get(VALUE_ATTR));
    }

    @Override
    public CurationsGroup getSettings(String settingsId) throws SettingsNotFoundException, InvalidSettingsException {
        // TODO cache relevance settings, including cache invalidation
        List<CurationSettings> curationsSettingsList;
        try {
            final SearchHits searchHits = client.prepareSearch(ENT_SEARCH_INDEX)
                .setQuery(new TermQueryBuilder(CURATIONS_GROUP_NAME, settingsId))
                .get()
                .getHits();
            if (searchHits.getTotalHits().value == 0) {
                throw new SettingsNotFoundException("Curation group " + settingsId + " not found");
            }
            curationsSettingsList = Arrays.stream(searchHits.getHits())
                .map(hit -> parseCurationsSettings(hit.getSourceAsMap()))
                .collect(Collectors.toList());
        } catch (IndexNotFoundException e) {
            IndexCreationService.ensureInternalIndex(client);
            curationsSettingsList = Collections.emptyList();
        }

        return new CurationsGroup(curationsSettingsList);
    }
}
