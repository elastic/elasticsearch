/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class FetchSourceContextTests extends AbstractSerializingTestCase<FetchSourceContext> {
    @Override
    protected FetchSourceContext doParseInstance(XContentParser parser) throws IOException {
        return FetchSourceContext.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<FetchSourceContext> instanceReader() {
        return FetchSourceContext::new;
    }

    @Override
    protected FetchSourceContext createTestInstance() {
        return new FetchSourceContext(
            true,
            randomArray(0, 5, String[]::new, () -> randomAlphaOfLength(5)),
            randomArray(0, 5, String[]::new, () -> randomAlphaOfLength(5))
        );
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
}
