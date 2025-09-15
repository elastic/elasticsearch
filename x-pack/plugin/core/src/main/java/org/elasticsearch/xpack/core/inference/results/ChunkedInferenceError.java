/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.xcontent.XContent;

import java.util.Collections;
import java.util.Iterator;

public record ChunkedInferenceError(Exception exception) implements ChunkedInference {

    @Override
    public Iterator<Chunk> chunksAsByteReference(XContent xcontent) {
        return Collections.emptyIterator();
    }
}
