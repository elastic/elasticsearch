/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.translog.Translog;

public interface XContentMeteringParserDecoratorSupplier {
    XContentMeteringParserDecoratorSupplier NOOP = new XContentMeteringParserDecoratorSupplier() {
    };

    /**
     * @return a parser decorator
     */
    default XContentMeteringParserDecorator newMeteringParserDecorator(IndexRequest request) {
        return XContentMeteringParserDecorator.NOOP;
    }

    /**
     * @return a parser decorator
     */
    default XContentMeteringParserDecorator newMeteringParserDecorator(Translog.Index index) {
        return XContentMeteringParserDecorator.NOOP;
    }
}
