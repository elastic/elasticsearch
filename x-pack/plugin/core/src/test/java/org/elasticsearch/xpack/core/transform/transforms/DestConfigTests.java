/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DestConfigTests extends AbstractSerializingTransformTestCase<DestConfig> {

    private boolean lenient;

    public static DestConfig randomDestConfig() {
        return new DestConfig(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomList(5, DestAliasTests::randomDestAlias),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomFrom("index", "create")
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

    @Override
    protected DestConfig mutateInstanceForVersion(DestConfig instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }

    public static DestConfig mutateForVersion(DestConfig instance, TransportVersion version) {
        if (version.supports(DestConfig.TRANSFORM_DEST_WRITE_ACTION)) {
            return instance;
        } else {
            if (instance.getWriteAction() == null) {
                return instance;
            }
            // Re-create without writeAction, preserving the raw aliases value.
            // We need the raw aliases field (null vs empty list matters for equals), but getAliases() normalizes null to List.of().
            // Access the raw field via reflection.
            List<DestAlias> rawAliases;
            try {
                var field = DestConfig.class.getDeclaredField("aliases");
                field.setAccessible(true);
                @SuppressWarnings("unchecked")
                List<DestAlias> value = (List<DestAlias>) field.get(instance);
                rawAliases = value;
            } catch (ReflectiveOperationException e) {
                throw new AssertionError(e);
            }
            return new DestConfig(instance.getIndex(), rawAliases, instance.getPipeline());
        }
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

    public void testInvalidWriteAction() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DestConfig("my-index", null, null, "invalid")
        );
        assertThat(e.getMessage(), containsString("invalid write_action [invalid]"));
    }

    public void testWriteActionCreate() throws IOException {
        String json = "{ \"index\": \"my-index\", \"write_action\": \"create\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            DestConfig dest = DestConfig.fromXContent(parser, false);
            assertThat(dest.getWriteAction(), equalTo("create"));
            assertThat(dest.getWriteOpType(), equalTo(DocWriteRequest.OpType.CREATE));
        }
    }

    public void testWriteActionIndex() throws IOException {
        String json = "{ \"index\": \"my-index\", \"write_action\": \"index\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            DestConfig dest = DestConfig.fromXContent(parser, false);
            assertThat(dest.getWriteAction(), equalTo("index"));
            assertThat(dest.getWriteOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        }
    }

    public void testWriteActionDefaultIsNull() throws IOException {
        String json = "{ \"index\": \"my-index\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            DestConfig dest = DestConfig.fromXContent(parser, false);
            assertThat(dest.getWriteAction(), is(nullValue()));
            assertThat(dest.getWriteOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        }
    }
}
