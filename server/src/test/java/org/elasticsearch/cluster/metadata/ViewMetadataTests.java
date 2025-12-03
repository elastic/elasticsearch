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
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomName;
import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomView;

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
        Map<String, View> views = new HashMap<>(instance.views());
        views.replaceAll((name, view) -> randomView(name));
        return new ViewMetadata(views);
    }

    @Override
    protected ViewMetadata createXContextTestInstance(XContentType xContentType) {
        return randomViewMetadata();
    }

    private static ViewMetadata randomViewMetadata() {
        int numViews = randomIntBetween(8, 64);
        Map<String, View> views = new HashMap<>(numViews);
        for (int i = 0; i < numViews; i++) {
            final String name = randomName();
            views.put(name, randomView(name));
        }
        return new ViewMetadata(views);
    }

    @Override
    protected Writeable.Reader<ViewMetadata> instanceReader() {
        return ViewMetadata::readFromStream;
    }
}
