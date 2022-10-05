/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance.curations;

import java.util.List;

/**
 * Holds curation settings, including the conditions needed to execute the curation, the pinned documents and the hidden documents.
 */
public record CurationSettings(List<DocumentReference> pinnedDocs, List<DocumentReference> hiddenDocs, List<Condition> conditions) {

    public record DocumentReference(String id, String index) {
        public DocumentReference(String id, String index) {
            if (id == null) {
                throw new IllegalArgumentException("Document ID must be specified");
            }
            if (index == null) {
                throw new IllegalArgumentException("Index must be specified");
            }

            this.id = id;
            this.index = index;
        }

    }

}
