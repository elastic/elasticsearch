/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.view.ViewTests.randomView;
import static org.hamcrest.Matchers.equalTo;

public class ViewMetadataTests extends AbstractChunkedSerializingTestCase<ViewMetadata> {

    @Override
    protected ViewMetadata doParseInstance(XContentParser parser) throws IOException {
        return ViewMetadata.fromXContent(parser);
    }

    @Override
    protected ViewMetadata createTestInstance() {
        return randomViewMetadata(randomFrom(XContentType.values()));
    }

    @Override
    protected ViewMetadata mutateInstance(ViewMetadata instance) {
        HashMap<String, View> views = new HashMap<>(instance.views());
        views.replaceAll((name, view) -> randomView(randomFrom(XContentType.values())));
        return new ViewMetadata(views);
    }

    @Override
    protected ViewMetadata createXContextTestInstance(XContentType xContentType) {
        return randomViewMetadata(xContentType);
    }

    private static ViewMetadata randomViewMetadata(XContentType xContentType) {
        int numViews = randomIntBetween(8, 64);
        Map<String, View> views = Maps.newMapWithExpectedSize(numViews);
        for (int i = 0; i < numViews; i++) {
            View view = randomView(xContentType);
            views.put(randomAlphaOfLength(8), view);
        }
        return new ViewMetadata(views);
    }

    @Override
    protected Writeable.Reader<ViewMetadata> instanceReader() {
        return ViewMetadata::new;
    }

    @Override
    protected void assertEqualInstances(ViewMetadata expectedInstance, ViewMetadata newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertThat(newInstance.views().size(), equalTo(expectedInstance.views().size()));
        for (Map.Entry<String, View> entry : newInstance.views().entrySet()) {
            View actual = entry.getValue();
            View expected = expectedInstance.views().get(entry.getKey());
            ViewTests.assertEqualViews(expected, actual);
        }
    }
}
