/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.support.filtering;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

public class YamlFilteringGeneratorTests extends AbstractXContentFilteringTestCase {

    @Override
    protected XContentType getXContentType() {
        return XContentType.YAML;
    }

    @Override
    protected void assertFilterResult(XContentBuilder expected, XContentBuilder actual) {
        if (randomBoolean()) {
            assertXContentBuilderAsString(expected, actual);
        } else {
            assertXContentBuilderAsBytes(expected, actual);
        }
    }
}
