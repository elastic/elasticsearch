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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
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

public class DestConfigTests extends AbstractSerializingTransformTestCase<DestConfig> {

    private boolean lenient;

    public static DestConfig randomDestConfig() {
        return new DestConfig(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomList(5, DestAliasTests::randomDestAlias),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomFrom(DocWriteRequest.OpType.INDEX, DocWriteRequest.OpType.CREATE)
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
        if (version.supports(DestConfig.TRANSFORM_DEST_OP_TYPE)) {
            return instance;
        } else {
            if (instance.getOpType() == DocWriteRequest.OpType.INDEX) {
                // Default value — no change needed after stripping the field
                return instance;
            }
            // Serialize at the BWC version and deserialize to get the expected object (op_type field omitted by version guard).
            // This avoids needing access to the raw aliases field (null vs empty list matters for equals).
            try {
                return copyWriteable(instance, new NamedWriteableRegistry(List.of()), DestConfig::new, version);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
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

    public void testInvalidOpType() throws IOException {
        String json = "{ \"index\": \"my-index\", \"op_type\": \"invalid\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DestConfig.fromXContent(parser, false));
            assertThat(e.getCause().getMessage(), containsString("invalid op_type [invalid]"));
        }
    }

    public void testUnsupportedOpTypeUpdate() throws IOException {
        String json = "{ \"index\": \"my-index\", \"op_type\": \"update\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DestConfig.fromXContent(parser, false));
            assertThat(e.getCause().getMessage(), containsString("invalid op_type [update]"));
        }
    }

    public void testUnsupportedOpTypeDelete() throws IOException {
        String json = "{ \"index\": \"my-index\", \"op_type\": \"delete\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DestConfig.fromXContent(parser, false));
            assertThat(e.getCause().getMessage(), containsString("invalid op_type [delete]"));
        }
    }

    public void testOpTypeCreate() throws IOException {
        String json = "{ \"index\": \"my-index\", \"op_type\": \"create\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            DestConfig dest = DestConfig.fromXContent(parser, false);
            assertThat(dest.getOpType(), equalTo(DocWriteRequest.OpType.CREATE));
        }
    }

    public void testOpTypeIndex() throws IOException {
        String json = "{ \"index\": \"my-index\", \"op_type\": \"index\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            DestConfig dest = DestConfig.fromXContent(parser, false);
            assertThat(dest.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        }
    }

    public void testOpTypeDefaultIsIndex() throws IOException {
        String json = "{ \"index\": \"my-index\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            DestConfig dest = DestConfig.fromXContent(parser, false);
            assertThat(dest.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        }
    }
}
