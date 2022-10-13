/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.settings.curations;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryBuilder;
import org.elasticsearch.xpack.relevancesearch.settings.Settings;

import java.util.List;

/**
 * Holds curation settings, including the conditions needed to execute the curation, the pinned documents and the excluded documents.
 */
public record CurationSettings(List<DocumentReference> pinnedDocs, List<DocumentReference> excludedDocs, List<Condition> conditions)
    implements
        Settings {

    public record DocumentReference(String id, String index) {
        public DocumentReference(String id, String index) {
            if (Strings.isEmpty(id)) {
                throw new IllegalArgumentException("Document ID must be specified");
            }
            if (Strings.isEmpty(index)) {
                throw new IllegalArgumentException("Index must be specified");
            }

            this.id = id;
            this.index = index;
        }

    }

    public boolean match(RelevanceMatchQueryBuilder relevanceMatchQueryBuilder) {
        return conditions().stream().anyMatch(c -> c.match(relevanceMatchQueryBuilder));
    }

}
