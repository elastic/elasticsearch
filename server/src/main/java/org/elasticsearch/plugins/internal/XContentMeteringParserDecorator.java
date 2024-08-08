/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.index.mapper.ParsedDocument.DocumentSize;
import org.elasticsearch.xcontent.XContentParser;

public interface XContentMeteringParserDecorator extends XContentParserDecorator {
    /**
     * a default noop implementation
     */
    XContentMeteringParserDecorator NOOP = new XContentMeteringParserDecorator() {
        @Override
        public DocumentSize meteredDocumentSize() {
            return DocumentSize.UNKNOWN;
        }

        @Override
        public XContentParser decorate(XContentParser xContentParser) {
            return xContentParser;
        }
    };

    DocumentSize meteredDocumentSize();
}
