/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.remote;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.settings.InferenceHeadersAndBody;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;

class RemoteInferenceRequest implements Request {
    private final RemoteInferenceModel model;
    private final InferenceInputs inferenceInputs;
    private final URI uri;
    private final InferenceHeadersAndBody headersAndBody;
    private final Iterator<boolean[]> truncations;
    private final boolean[] currentTruncation;

    RemoteInferenceRequest(
        RemoteInferenceModel model,
        InferenceInputs inferenceInputs,
        URI uri,
        InferenceHeadersAndBody headersAndBody,
        Iterator<boolean[]> truncations,
        boolean[] currentTruncation
    ) {
        this.model = model;
        this.inferenceInputs = inferenceInputs;
        this.uri = uri;
        this.headersAndBody = headersAndBody;
        this.truncations = truncations;
        this.currentTruncation = currentTruncation;
    }

    RemoteInferenceRequest(RemoteInferenceModel model, InferenceInputs inferenceInputs, URI uri, InferenceHeadersAndBody headersAndBody) {
        this(model, inferenceInputs, uri, headersAndBody, Collections.emptyIterator(), new boolean[0]);
    }

    @Override
    public HttpRequest createHttpRequest() {
        var request = new HttpPost(getURI());

        headersAndBody.headers().forEach(request::addHeader);

        try (var builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.map(headersAndBody.body());
            builder.endObject();

            var byteEntity = new ByteArrayEntity(Strings.toString(builder).getBytes(StandardCharsets.UTF_8));
            request.setEntity(byteEntity);
            return new HttpRequest(request, getInferenceEntityId());
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse body into json.", e);
        }
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public Request truncate() {
        if(truncations.hasNext()) {
            return new RemoteInferenceRequest(model, inferenceInputs, uri, headersAndBody, truncations, truncations.next());
        } else {
            return this;
        }
    }

    @Override
    public boolean[] getTruncationInfo() {
        return currentTruncation;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public boolean isStreaming() {
        return inferenceInputs.stream();
    }
}
