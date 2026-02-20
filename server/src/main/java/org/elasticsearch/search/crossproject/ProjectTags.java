/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import java.util.Map;

/**
 * Project tags used for cross-project search routing.
 * @param tags the map of tags -- contains both built-in (Elastic-supplied) and custom user-defined tags.
 *             All built-in tags are prefixed with an underscore (_).
 */
public record ProjectTags(Map<String, String> tags) {
    public static final String PROJECT_ID_TAG = "_id";
    public static final String PROJECT_ALIAS = "_alias";
    public static final String PROJECT_TYPE_TAG = "_type";
    public static final String ORGANIZATION_ID_TAG = "_organization";

    public String projectId() {
        return tags.get(PROJECT_ID_TAG);
    }

    public String organizationId() {
        return tags.get(ORGANIZATION_ID_TAG);
    }

    public String projectType() {
        return tags.get(PROJECT_TYPE_TAG);
    }

    public String projectAlias() {
        return tags.get(PROJECT_ALIAS);
    }

    /**
     * Validate that all required tags are present.
     */
    public static void validateTags(String projectId, Map<String, String> tags) {
        if (false == tags.containsKey(PROJECT_ID_TAG)) {
            throw missingTagException(projectId, PROJECT_ID_TAG);
        }
        if (false == tags.containsKey(PROJECT_TYPE_TAG)) {
            throw missingTagException(projectId, PROJECT_TYPE_TAG);
        }
        if (false == tags.containsKey(ORGANIZATION_ID_TAG)) {
            throw missingTagException(projectId, ORGANIZATION_ID_TAG);
        }
        if (false == tags.containsKey(PROJECT_ALIAS)) {
            throw missingTagException(projectId, PROJECT_ALIAS);
        }
    }

    private static IllegalStateException missingTagException(String projectId, String tagKey) {
        return new IllegalStateException("Project configuration for [" + projectId + "] is missing required tag [" + tagKey + "]");
    }
}
