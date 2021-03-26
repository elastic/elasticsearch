/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.util;

import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

public class PageParamsTests extends AbstractXContentTestCase<PageParams> {

    @Override
    protected PageParams doParseInstance(XContentParser parser) {
        return PageParams.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected PageParams createTestInstance() {
        Integer from = randomBoolean() ? randomInt() : null;
        Integer size = randomBoolean() ? randomInt() : null;
        return new PageParams(from, size);
    }
}
