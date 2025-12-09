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
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomName;
import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomView;
import static org.hamcrest.Matchers.equalTo;

public class ViewTests extends AbstractXContentSerializingTestCase<View> {

    @Override
    protected View doParseInstance(XContentParser parser) throws IOException {
        return View.fromXContent(parser);
    }

    @Override
    protected View createTestInstance() {
        return randomView(randomName());
    }

    @Override
    protected View mutateInstance(View instance) {
        return randomValueOtherThan(instance, () -> randomView(instance.name()));
    }

    @Override
    protected View createXContextTestInstance(XContentType xContentType) {
        return randomView(randomName());
    }

    @Override
    protected Writeable.Reader<View> instanceReader() {
        return View::new;
    }

    @Override
    protected void assertEqualInstances(View expectedInstance, View newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertEqualViews(expectedInstance, newInstance);
    }

    public static void assertEqualViews(View expectedInstance, View newInstance) {
        assertThat(newInstance.query(), equalTo(expectedInstance.query()));
    }
}
