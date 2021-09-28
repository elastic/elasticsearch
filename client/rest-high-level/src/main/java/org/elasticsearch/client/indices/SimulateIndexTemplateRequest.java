/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;

/**
 * A request to simulate matching a provided index name and an optional new index template against the existing index templates.
 */
public class SimulateIndexTemplateRequest extends TimedRequest {

    private String indexName;

    @Nullable
    private PutComposableIndexTemplateRequest indexTemplateV2Request;

    public SimulateIndexTemplateRequest(String indexName) {
        if (Strings.isNullOrEmpty(indexName)) {
            throw new IllegalArgumentException("index name cannot be null or empty");
        }
        this.indexName = indexName;
    }

    /**
     * Return the index name for which we simulate the index template matching.
     */
    public String indexName() {
        return indexName;
    }

    /**
     * Set the index name to simulate template matching against the index templates in the system.
     */
    public SimulateIndexTemplateRequest indexName(String indexName) {
        if (Strings.isNullOrEmpty(indexName)) {
            throw new IllegalArgumentException("index name cannot be null or empty");
        }
        this.indexName = indexName;
        return this;
    }

    /**
     * An optional new template request will be part of the index template simulation.
     */
    @Nullable
    public PutComposableIndexTemplateRequest indexTemplateV2Request() {
        return indexTemplateV2Request;
    }

    /**
     * Optionally, define a new template request which will included in the index simulation as if it was an index template stored in the
     * system. The new template will be validated just as a regular, standalone, live, new index template request.
     */
    public SimulateIndexTemplateRequest indexTemplateV2Request(@Nullable PutComposableIndexTemplateRequest indexTemplateV2Request) {
        this.indexTemplateV2Request = indexTemplateV2Request;
        return this;
    }
}
