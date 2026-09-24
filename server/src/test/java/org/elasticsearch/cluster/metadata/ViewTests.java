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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ViewTests extends AbstractXContentSerializingTestCase<View> {

    @Override
    protected View doParseInstance(XContentParser parser) throws IOException {
        return View.fromXContent(parser);
    }

    @Override
    protected View createTestInstance() {
        return new View(randomIdentifier(), randomQuery(), randomDescription(), randomBoolean());
    }

    @Override
    protected View mutateInstance(View instance) {
        var name = instance.getName();
        var query = instance.query();
        var description = instance.description();
        var isSystem = instance.isSystem();
        switch (between(0, 3)) {
            case 0 -> name = randomValueOtherThan(name, ESTestCase::randomIdentifier);
            case 1 -> query = randomValueOtherThan(query, ViewTests::randomQuery);
            case 2 -> description = randomValueOtherThan(description, ViewTests::randomDescription);
            case 3 -> isSystem = !isSystem;
            default -> throw new AssertionError("Unexpected randomisation branch");
        }
        return new View(name, query, description, isSystem);
    }

    public static String randomQuery() {
        return "FROM " + randomIdentifier();
    }

    private static String randomDescription() {
        return randomBoolean() ? randomAlphaOfLength(10) : null;
    }

    @Override
    protected Writeable.Reader<View> instanceReader() {
        return View::new;
    }

    @Override
    protected void assertEqualInstances(View expectedInstance, View newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertThat(newInstance, equalTo(expectedInstance));
    }
}
