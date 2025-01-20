/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class SubGroupTests extends ESTestCase {
    public void testToXContent() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        // tag::noformat
        XContentBuilder expectedRequest = XContentFactory.contentBuilder(contentType)
            .startObject()
                .startObject("transaction.name")
                    .startObject("basket")
                        .field("count", 7L)
                    .endObject()
                .endObject()
            .endObject();
        // end::noformat

        XContentBuilder actualRequest = XContentFactory.contentBuilder(contentType);
        actualRequest.startObject();
        SubGroup g = SubGroup.root("transaction.name").addCount("basket", 7L);
        g.toXContent(actualRequest, ToXContent.EMPTY_PARAMS);
        actualRequest.endObject();

        assertToXContentEquivalent(BytesReference.bytes(expectedRequest), BytesReference.bytes(actualRequest), contentType);
    }

    public void testMergeNoCommonRoot() {
        SubGroup root1 = SubGroup.root("transaction.name");
        SubGroup root2 = SubGroup.root("service.name");

        SubGroup toMerge = root1.copy();

        toMerge.merge(root2);

        assertEquals(root1, toMerge);
    }

    public void testMergeIdenticalTree() {
        SubGroup g = SubGroup.root("transaction.name");
        g.addCount("basket", 5L);
        g.addCount("checkout", 7L);

        SubGroup g2 = g.copy();

        g.merge(g2);

        assertEquals(Long.valueOf(10L), g.getCount("basket"));
        assertEquals(Long.valueOf(14L), g.getCount("checkout"));
    }

    public void testMergeMixedTree() {
        SubGroup g1 = SubGroup.root("transaction.name");
        g1.addCount("basket", 5L);
        g1.addCount("checkout", 7L);

        SubGroup g2 = SubGroup.root("transaction.name");
        g2.addCount("catalog", 8L);
        g2.addCount("basket", 5L);
        g2.addCount("checkout", 2L);

        g1.merge(g2);

        assertEquals(Long.valueOf(8L), g1.getCount("catalog"));
        assertEquals(Long.valueOf(10L), g1.getCount("basket"));
        assertEquals(Long.valueOf(9L), g1.getCount("checkout"));
    }
}
