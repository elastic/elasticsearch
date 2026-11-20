/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.xcontent.XContentParser;

public interface XContentParserDecorator {
    XContentParserDecorator NOOP = (parser, mapping) -> parser;

    /**
     * Decorates the parser used to read a document's source. The {@code mapping} is unused by the implementations in this repository,
     * but is used by {@code MeteringDocumentParserProvider} in serverless, so it must be kept.
     */
    XContentParser decorate(XContentParser xContentParser, Mapping mapping);
}
