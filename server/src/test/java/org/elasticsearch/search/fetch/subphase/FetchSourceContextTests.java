/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class FetchSourceContextTests extends AbstractXContentSerializingTestCase<FetchSourceContext> {
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
            randomBoolean() ? null : randomBoolean(),
            randomArray(0, 5, String[]::new, () -> randomAlphaOfLength(5)),
            randomArray(0, 5, String[]::new, () -> randomAlphaOfLength(5))
        );
    }

    @Override
    protected FetchSourceContext mutateInstance(FetchSourceContext instance) {
        return switch (randomInt(2)) {
            case 0 -> FetchSourceContext.of(
                true,
                instance.includeVectors() != null ? instance.includeVectors() == false : randomBoolean(),
                instance.includes(),
                instance.excludes()
            );
            case 1 -> FetchSourceContext.of(
                true,
                instance.includeVectors(),
                randomArray(instance.includes().length + 1, instance.includes().length + 5, String[]::new, () -> randomAlphaOfLength(5)),
                instance.excludes()
            );
            case 2 -> FetchSourceContext.of(
                true,
                instance.includeVectors(),
                instance.includes(),
                randomArray(instance.excludes().length + 1, instance.excludes().length + 5, String[]::new, () -> randomAlphaOfLength(5))
            );
            default -> throw new AssertionError("cannot reach");
        };
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
        if (expectedInstance == FetchSourceContext.FETCH_SOURCE || expectedInstance == FetchSourceContext.DO_NOT_FETCH_SOURCE) {
            assertSame(expectedInstance, newInstance);
        } else {
            super.assertEqualInstances(expectedInstance, newInstance);
        }
    }
}
