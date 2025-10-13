/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.xcontent.XContentParser;

public interface XContentMeteringParserDecorator extends XContentParserDecorator {
    long UNKNOWN_SIZE = -1;
    /**
     * a default noop implementation
     */
    XContentMeteringParserDecorator NOOP = new XContentMeteringParserDecorator() {
        @Override
        public long meteredDocumentSize() {
            return UNKNOWN_SIZE;
        }

        @Override
        public XContentParser decorate(XContentParser xContentParser) {
            return xContentParser;
        }
    };

    long meteredDocumentSize();
}
