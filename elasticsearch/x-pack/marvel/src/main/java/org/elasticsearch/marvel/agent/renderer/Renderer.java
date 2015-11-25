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
 * Renderers are used to render documents using a given OutputStream.
 */
public interface Renderer<T> {

    void render(T document, XContentType xContentType, OutputStream os) throws IOException;
}
