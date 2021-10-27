/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Helper to build a {@link RestHighLevelClient}, allowing setting the low-level client that
 * should be used as well as whether API compatibility should be used.
 * @deprecated The High Level Rest Client is deprecated in favor of the
 * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/introduction.html">
 * Elasticsearch Java API Client</a>
 */
@Deprecated(since = "7.16.0", forRemoval = true)
@SuppressWarnings("removal")
public class RestHighLevelClientBuilder {
    private final RestClient restClient;
    private CheckedConsumer<RestClient, IOException> closeHandler = RestClient::close;
    private List<NamedXContentRegistry.Entry> namedXContentEntries = Collections.emptyList();
    private Boolean apiCompatibilityMode = null;

    public RestHighLevelClientBuilder(RestClient restClient) {
        this.restClient = restClient;
    }

    public RestHighLevelClientBuilder closeHandler(CheckedConsumer<RestClient, IOException> closeHandler) {
        this.closeHandler = closeHandler;
        return this;
    }

    public RestHighLevelClientBuilder namedXContentEntries(List<NamedXContentRegistry.Entry> namedXContentEntries) {
        this.namedXContentEntries = namedXContentEntries;
        return this;
    }

    public RestHighLevelClientBuilder setApiCompatibilityMode(Boolean enabled) {
        this.apiCompatibilityMode = enabled;
        return this;
    }

    public RestHighLevelClient build() {
        return new RestHighLevelClient(this.restClient, this.closeHandler, this.namedXContentEntries, this.apiCompatibilityMode);
    }
}
