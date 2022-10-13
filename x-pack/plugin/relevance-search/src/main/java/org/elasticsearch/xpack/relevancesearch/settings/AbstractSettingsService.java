/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.settings;

import org.elasticsearch.client.internal.Client;

public abstract class AbstractSettingsService<S extends Settings> implements SettingsService<S> {
    public static final String ENT_SEARCH_INDEX = ".ent-search";

    protected final Client client;

    public AbstractSettingsService(final Client client) {
        this.client = client;
    }

}
