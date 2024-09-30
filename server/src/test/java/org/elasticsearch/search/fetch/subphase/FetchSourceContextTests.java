/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class FetchSourceContextTests extends AbstractBWCSerializationTestCase<FetchSourceContext> {
    @Override
    protected FetchSourceContext doParseInstance(XContentParser parser) throws IOException {
        return FetchSourceContext.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<FetchSourceContext> instanceReader() {
        return FetchSourceContext::readFrom;
    }

    @Override
    protected FetchSourceContext createTestInstance() {
        return FetchSourceContext.of(
            true,
            randomArray(0, 5, String[]::new, () -> randomAlphaOfLength(5)),
            randomArray(0, 5, String[]::new, () -> randomAlphaOfLength(5)),
            randomOptionalBoolean()
        );
    }

    @Override
    protected FetchSourceContext mutateInstance(FetchSourceContext instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public void testFromXContentException() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        int value = randomInt();
        builder.value(value);
        XContentParser parser = createParser(builder);
        ParsingException exception = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        assertThat(
            exception.getMessage(),
            containsString("Expected one of [VALUE_BOOLEAN, VALUE_STRING, START_ARRAY, START_OBJECT] but found [VALUE_NUMBER]")
        );

    }

    @Override
    protected void assertEqualInstances(FetchSourceContext expectedInstance, FetchSourceContext newInstance) {
        if (expectedInstance == FetchSourceContext.FETCH_SOURCE
            || expectedInstance == FetchSourceContext.DO_NOT_FETCH_SOURCE
            || expectedInstance == FetchSourceContext.FETCH_SOURCE_WITH_VECTORS) {
            assertSame(expectedInstance, newInstance);
        } else {
            super.assertEqualInstances(expectedInstance, newInstance);
        }
    }

    @Override
    protected FetchSourceContext mutateInstanceForVersion(FetchSourceContext instance, TransportVersion version) {
        if (version.before(TransportVersions.HIDE_VECTORS_IN_SOURCE)) {
            // Deserialization logic sets includeVectors to true for old transport versions
            instance = FetchSourceContext.of(instance.fetchSource(), instance.includes(), instance.excludes(), true);
        }
        return instance;
    }

    @Override
    protected void assertOnBWCObject(FetchSourceContext bwcSerializedObject, FetchSourceContext testInstance, TransportVersion version) {
        if (bwcSerializedObject == FetchSourceContext.FETCH_SOURCE
            || bwcSerializedObject == FetchSourceContext.DO_NOT_FETCH_SOURCE
            || bwcSerializedObject == FetchSourceContext.FETCH_SOURCE_WITH_VECTORS) {
            assertSame(version.toString(), bwcSerializedObject, testInstance);
        } else {
            super.assertOnBWCObject(bwcSerializedObject, testInstance, version);
        }
    }
}
