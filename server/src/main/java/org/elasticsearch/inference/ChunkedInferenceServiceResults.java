/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContent;

import java.util.AbstractMap;
import java.util.Iterator;

public interface ChunkedInferenceServiceResults extends InferenceServiceResults {

    Iterator<Chunk> chunksAsMatchedTextAndByteReference(XContent xcontent);

    /**
     * A chunk of inference results containing matched text and the bytes reference.
     * @param matchedText
     * @param bytesReference
     */
    record Chunk(String matchedText, BytesReference bytesReference) {}
}
