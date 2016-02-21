/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer;

import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@code Renderer}s are used to render documents using a given OutputStream.
 * <p>
 * Each {@code Renderer} can be thought of as a generator of a unique document <em>type</em> within the resulting ES index. For example,
 * there will be details about shards, which requires a unique document type and there will also be details about indices, which requires
 * their own unique documents.
 *
 * @see AbstractRenderer
 */
public interface Renderer<T> {
    /**
     * Convert the given {@code document} type into something that can be sent to the monitoring cluster.
     *
     * @param document The arbitrary document (e.g., details about a shard)
     * @param xContentType The rendered content type (e.g., JSON)
     * @param os The buffer
     * @throws IOException if any unexpected error occurs
     */
    void render(T document, XContentType xContentType, OutputStream os) throws IOException;
}
