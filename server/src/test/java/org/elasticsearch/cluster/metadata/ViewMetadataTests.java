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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ViewMetadataTests extends AbstractChunkedSerializingTestCase<ViewMetadata> {

    @Override
    protected ViewMetadata doParseInstance(XContentParser parser) throws IOException {
        return ViewMetadata.fromXContent(parser);
    }

    @Override
    protected ViewMetadata createTestInstance() {
        return new ViewMetadata(randomMap(0, 64, () -> {
            var name = randomIdentifier();
            return new Tuple<>(name, new View(name, ViewTests.randomQuery()));
        }));
    }

    @Override
    protected ViewMetadata mutateInstance(ViewMetadata instance) {
        Map<String, View> views = new HashMap<>(instance.views());
        var mutation = between(0, 2);
        if (mutation == 0 || views.isEmpty()) {
            var name = randomIdentifier();
            views.put(name, new View(name, ViewTests.randomQuery()));
        } else if (mutation == 1) {
            var name = randomFrom(views.keySet());
            views.remove(name);
        } else if (mutation == 2) {
            var name = randomFrom(views.keySet());
            views.compute(name, (n, view) -> new View(name, randomValueOtherThan(view.query(), ViewTests::randomQuery)));
        }
        return new ViewMetadata(views);
    }

    private static View randomView(String name) {
        String query = "FROM " + randomAlphaOfLength(10);
        return new View(name, query);
    }

    @Override
    protected Writeable.Reader<ViewMetadata> instanceReader() {
        return ViewMetadata::readFromStream;
    }
}
