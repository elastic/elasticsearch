/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ChunkedToXContent.wrapAsToXContent;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.endObject;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.startObject;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class ChunkedToXContentTests extends ESTestCase {

    public void testWrapAsToXContentProducesEqualInstances() throws IOException {
        ChunkedToXContentObject object = params -> Iterators.concat(
            startObject(),
            Iterators.single((builder, p) -> builder.field("test", 42)),
            endObject()
        );

        ToXContent first = wrapAsToXContent(object);
        ToXContent second = wrapAsToXContent(object);

        for (var xContentType : XContentType.values()) {
            assertToXContentEquivalent(
                XContentHelper.toXContent(first, xContentType, false),
                XContentHelper.toXContent(second, xContentType, false),
                xContentType
            );
        }
    }
}
