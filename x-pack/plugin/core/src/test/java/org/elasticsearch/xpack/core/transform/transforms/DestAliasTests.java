/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DestAliasTests extends AbstractSerializingTransformTestCase<DestAlias> {

    private boolean lenient;

    public static DestAlias randomDestAlias() {
        return new DestAlias(randomAlphaOfLength(10), randomBoolean());
    }

    @Before
    public void setRandomFeatures() {
        lenient = randomBoolean();
    }

    @Override
    protected DestAlias doParseInstance(XContentParser parser) throws IOException {
        return DestAlias.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected DestAlias createTestInstance() {
        return randomDestAlias();
    }

    @Override
    protected DestAlias mutateInstance(DestAlias instance) {
        return new DestAlias(instance.getAlias() + "-x", instance.isMoveOnCreation() == false);
    }

    @Override
    protected Reader<DestAlias> instanceReader() {
        return DestAlias::new;
    }

    public void testFailOnEmptyAlias() throws IOException {
        boolean lenient2 = randomBoolean();
        String json = "{ \"alias\": \"\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            DestAlias destAlias = DestAlias.fromXContent(parser, lenient2);
            assertThat(destAlias.getAlias(), is(emptyString()));
            ValidationException validationException = destAlias.validate(null);
            assertThat(validationException, is(notNullValue()));
            assertThat(validationException.getMessage(), containsString("dest.aliases.alias must not be empty"));
        }
    }
}
