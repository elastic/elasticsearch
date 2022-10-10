/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.analytics;

import java.util.Date;

/**
 * Describes the relevance search event information logged to the relevance datastream
 * TODO - convert POJO to json
 */
public class RelevanceSearchEvent {

    public final Date timestamp;
    public final String relevanceQuery;
    public final String relevanceSettingsId;
    public final String curationId;

    public RelevanceSearchEvent(Date timestamp, String relevanceQuery, String relevanceSettingsId, String curationId) {
        this.timestamp = timestamp;
        this.relevanceQuery = relevanceQuery;
        this.relevanceSettingsId = relevanceSettingsId;
        this.curationId = curationId;
    }
}
