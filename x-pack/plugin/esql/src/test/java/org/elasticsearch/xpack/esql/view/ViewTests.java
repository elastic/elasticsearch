/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

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
        return randomView(instance.name());
    }

    @Override
    protected View createXContextTestInstance(XContentType xContentType) {
        return randomView(randomName());
    }

    public static String randomName() {
        return randomAlphaOfLength(8);
    }

    public static View randomView(String name) {
        String query = "FROM " + randomAlphaOfLength(10);
        return new View(name, query);
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
