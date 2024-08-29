/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.index.mapper.MapperService;

/**
 * An interface to provide instances of document parsing observer and reporter
 */
public interface DocumentParsingProvider {
    DocumentParsingProvider EMPTY_INSTANCE = new DocumentParsingProvider() {
    };

    /**
     * @return an instance of a reporter to use when parsing has been completed and indexing successful
     */
    default DocumentSizeReporter newDocumentSizeReporter(
        String indexName,
        MapperService mapperService,
        DocumentSizeAccumulator documentSizeAccumulator
    ) {
        return DocumentSizeReporter.EMPTY_INSTANCE;
    }

    /**
     * @return a new instance of DocumentSizeAccumulator
     */
    default DocumentSizeAccumulator createDocumentSizeAccumulator() {
        return DocumentSizeAccumulator.EMPTY_INSTANCE;
    }

    /**
     * @return an observer
     */
    default <T> XContentMeteringParserDecorator newMeteringParserDecorator(DocWriteRequest<T> request) {
        return XContentMeteringParserDecorator.NOOP;
    }
}
