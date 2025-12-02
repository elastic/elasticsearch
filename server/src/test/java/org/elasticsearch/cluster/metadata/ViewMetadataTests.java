/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomName;
import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomView;
import static org.hamcrest.Matchers.equalTo;

public class ViewMetadataTests extends AbstractChunkedSerializingTestCase<ViewMetadata> {

    @Override
    protected ViewMetadata doParseInstance(XContentParser parser) throws IOException {
        return ViewMetadata.fromXContent(parser);
    }

    @Override
    protected ViewMetadata createTestInstance() {
        return randomViewMetadata();
    }

    @Override
    protected ViewMetadata mutateInstance(ViewMetadata instance) {
        ArrayList<View> views = new ArrayList<>(instance.views());
        views.replaceAll((view) -> randomView(randomName()));
        return new ViewMetadata(views);
    }

    @Override
    protected ViewMetadata createXContextTestInstance(XContentType xContentType) {
        return randomViewMetadata();
    }

    private static ViewMetadata randomViewMetadata() {
        int numViews = randomIntBetween(8, 64);
        List<View> views = new ArrayList<>(numViews);
        for (int i = 0; i < numViews; i++) {
            views.add(randomView(randomName()));
        }
        return new ViewMetadata(views);
    }

    @Override
    protected Writeable.Reader<ViewMetadata> instanceReader() {
        return ViewMetadata::readFromStream;
    }

    @Override
    protected void assertEqualInstances(ViewMetadata expectedInstance, ViewMetadata newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertThat(newInstance.views().size(), equalTo(expectedInstance.views().size()));
        for (View actual : newInstance.views()) {
            View expected = expectedInstance.getView(actual.name());
            ViewTests.assertEqualViews(expected, actual);
        }
    }
}
