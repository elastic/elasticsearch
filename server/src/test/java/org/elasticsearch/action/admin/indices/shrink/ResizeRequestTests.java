/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.shrink;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.action.admin.indices.create.CreateIndexRequestTests.assertAliasesEqual;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class ResizeRequestTests extends AbstractWireSerializingTestCase<ResizeRequest> {

    public void testCopySettingsValidation() {
        runTestCopySettingsValidation(false, r -> {
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, r::get);
            assertThat(e, hasToString(containsString("[copySettings] can not be explicitly set to [false]")));
        });

        runTestCopySettingsValidation(null, r -> assertNull(r.get().getCopySettings()));
        runTestCopySettingsValidation(true, r -> assertTrue(r.get().getCopySettings()));
    }

    private void runTestCopySettingsValidation(final Boolean copySettings, final Consumer<Supplier<ResizeRequest>> consumer) {
        consumer.accept(() -> {
            final ResizeRequest request = new ResizeRequest();
            request.setCopySettings(copySettings);
            return request;
        });
    }

    public void testToXContent() throws IOException {
        {
            ResizeRequest request = new ResizeRequest("target", "source");
            String actualRequestBody = Strings.toString(request);
            assertEquals("{\"settings\":{},\"aliases\":{}}", actualRequestBody);
        }
        {
            ResizeRequest request = new ResizeRequest("target", "source");
            request.setMaxPrimaryShardSize(new ByteSizeValue(100, ByteSizeUnit.MB));
            String actualRequestBody = Strings.toString(request);
            assertEquals("{\"settings\":{},\"aliases\":{},\"max_primary_shard_size\":\"100mb\"}", actualRequestBody);
        }
        {
            ResizeRequest request = new ResizeRequest();
            CreateIndexRequest target = new CreateIndexRequest("target");
            Alias alias = new Alias("test_alias");
            alias.routing("1");
            alias.filter("{\"term\":{\"year\":2016}}");
            alias.writeIndex(true);
            target.alias(alias);
            Settings.Builder settings = Settings.builder();
            settings.put(SETTING_NUMBER_OF_SHARDS, 10);
            target.settings(settings);
            request.setTargetIndex(target);
            String actualRequestBody = Strings.toString(request);
            String expectedRequestBody = "{\"settings\":{\"index\":{\"number_of_shards\":\"10\"}}," +
                    "\"aliases\":{\"test_alias\":{\"filter\":{\"term\":{\"year\":2016}},\"routing\":\"1\",\"is_write_index\":true}}}";
            assertEquals(expectedRequestBody, actualRequestBody);
        }
    }

    public void testToAndFromXContent() throws IOException {
        final ResizeRequest resizeRequest = createTestInstance();

        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(resizeRequest, xContentType, EMPTY_PARAMS, humanReadable);

        ResizeRequest parsedResizeRequest = new ResizeRequest(
            randomValueOtherThan(resizeRequest.getTargetIndexRequest().index(), () -> randomAlphaOfLength(5)),
            randomValueOtherThan(resizeRequest.getSourceIndex(), () -> randomAlphaOfLength(5))
        );
        try (XContentParser xParser = createParser(xContentType.xContent(), originalBytes)) {
            parsedResizeRequest.fromXContent(xParser);
        }

        // these are not expected to be equal in the general case because ResizeRequest.toXContent doesn't include everything
        assertNotEquals(resizeRequest, parsedResizeRequest);
        assertNotEquals(resizeRequest.getSourceIndex(), parsedResizeRequest.getSourceIndex());
        assertNotEquals(resizeRequest.getTargetIndexRequest(), parsedResizeRequest.getTargetIndexRequest());
        assertNotEquals(resizeRequest.getTargetIndexRequest().index(), parsedResizeRequest.getTargetIndexRequest().index());

        // but these are expected to be equal
        assertAliasesEqual(resizeRequest.getTargetIndexRequest().aliases(), parsedResizeRequest.getTargetIndexRequest().aliases());
        assertEquals(resizeRequest.getTargetIndexRequest().settings(), parsedResizeRequest.getTargetIndexRequest().settings());
        assertEquals(resizeRequest.getMaxPrimaryShardSize(), parsedResizeRequest.getMaxPrimaryShardSize());

        BytesReference finalBytes = toShuffledXContent(parsedResizeRequest, xContentType, EMPTY_PARAMS, humanReadable);
        ElasticsearchAssertions.assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
    }

    public void testSerializeRequest() throws IOException {
        ResizeRequest request = createTestInstance();
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesReference bytes = out.bytes();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(NetworkModule.getNamedWriteables());
        StreamInput wrap = new NamedWriteableAwareStreamInput(bytes.streamInput(), namedWriteableRegistry);
        ResizeRequest deserializedReq = new ResizeRequest(wrap);

        assertEquals(request.getSourceIndex(), deserializedReq.getSourceIndex());
        assertEquals(request.getTargetIndexRequest().settings(), deserializedReq.getTargetIndexRequest().settings());
        assertEquals(request.getTargetIndexRequest().aliases(), deserializedReq.getTargetIndexRequest().aliases());
        assertEquals(request.getCopySettings(), deserializedReq.getCopySettings());
        assertEquals(request.getResizeType(), deserializedReq.getResizeType());
        assertEquals(request.getMaxPrimaryShardSize(), deserializedReq.getMaxPrimaryShardSize());
    }

    @Override
    protected Writeable.Reader<ResizeRequest> instanceReader() { return ResizeRequest::new; }

    @Override
    protected ResizeRequest createTestInstance() {
        ResizeRequest resizeRequest = new ResizeRequest(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            resizeRequest.setTargetIndex(RandomCreateIndexGenerator.randomCreateIndexRequest());
        }
        if (randomBoolean()) {
            resizeRequest.setMaxPrimaryShardSize(new ByteSizeValue(randomIntBetween(1, 100)));
        }
        return resizeRequest;
    }
}
