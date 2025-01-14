/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.normalizer;

import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Defines a ScoreNormalizer parser that is able to parse {@link ScoreNormalizer}s
 * from {@link org.elasticsearch.xcontent.XContent}.
 */
@FunctionalInterface
public interface ScoreNormalizerParser<SN extends ScoreNormalizer> {

    /**
     * Creates a new {@link RetrieverBuilder} from the retriever held by the
     * {@link XContentParser}. The state on the parser contained in this context
     * will be changed as a side effect of this method call. The
     * {@link RetrieverParserContext} tracks usage of retriever features and
     * queries when available.
     */
    SN fromXContent(XContentParser parser) throws IOException;
}
