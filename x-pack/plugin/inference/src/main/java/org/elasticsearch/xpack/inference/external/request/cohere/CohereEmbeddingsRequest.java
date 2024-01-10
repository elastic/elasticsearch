/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere;

import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.net.URI;

public class CohereEmbeddingsRequest implements Request {

    public CohereEmbeddingsRequest(Truncator truncator, Object account, Truncator.TruncationResult input, Object taskSettings) {

    }

    @Override
    public HttpRequestBase createRequest() {
        return null;
    }

    @Override
    public URI getURI() {
        return null;
    }

    @Override
    public Request truncate() {
        return null;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return new boolean[0];
    }
}
