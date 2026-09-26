/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.highlight;

import org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils;

import java.io.IOException;

interface ChunkContentExtractor {
    String getContent(SemanticTextChunkUtils.OffsetAndScore chunk) throws IOException;
}
