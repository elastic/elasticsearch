/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance;

public class RelevanceSettings {

    private final String TYPE = "relevance_settings";
    private final String QUERY_TYPE = "combined_fields";
    private String name;

    private QueryConfiguration queryConfiguration;

    public QueryConfiguration getQueryConfiguration() { return queryConfiguration; }

    public void setQueryConfiguration(QueryConfiguration queryConfiguration) { this.queryConfiguration = queryConfiguration; }



}
