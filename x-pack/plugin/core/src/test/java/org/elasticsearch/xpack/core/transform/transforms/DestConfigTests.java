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

public class DestConfigTests extends AbstractSerializingTransformTestCase<DestConfig> {

    private boolean lenient;

    public static DestConfig randomDestConfig() {
        return new DestConfig(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomList(5, DestAliasTests::randomDestAlias),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
    }

    @Before
    public void setRandomFeatures() {
        lenient = randomBoolean();
    }

    @Override
    protected DestConfig doParseInstance(XContentParser parser) throws IOException {
        return DestConfig.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected DestConfig createTestInstance() {
        return randomDestConfig();
    }

    @Override
    protected DestConfig mutateInstance(DestConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<DestConfig> instanceReader() {
        return DestConfig::new;
    }

    public void testFailOnEmptyIndex() throws IOException {
        boolean lenient2 = randomBoolean();
        String json = "{ \"index\": \"\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            DestConfig dest = DestConfig.fromXContent(parser, lenient2);
            assertThat(dest.getIndex(), is(emptyString()));
            ValidationException validationException = dest.validate(null);
            assertThat(validationException, is(notNullValue()));
            assertThat(validationException.getMessage(), containsString("dest.index must not be empty"));
        }
    }
}
