/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RolloverRequestTests extends ESTestCase {
    private NamedWriteableRegistry writeableRegistry;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        writeableRegistry = new NamedWriteableRegistry(IndicesModule.getNamedWriteables());
    }

    public void testConditionsParsing() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("conditions")
            .field("max_age", "10d")
            .field("max_docs", 100)
            .field("max_size", "45gb")
            .field("max_primary_shard_size", "55gb")
            .field("max_primary_shard_docs", 10)
            .field("min_age", "10d")
            .field("min_docs", 100)
            .field("min_size", "45gb")
            .field("min_primary_shard_size", "55gb")
            .field("min_primary_shard_docs", 10)
            .endObject()
            .endObject();
        try (var parser = createParser(builder)) {
            request.fromXContent(parser);
        }
        Map<String, Condition<?>> conditions = request.getConditions().getConditions();
        assertThat(conditions.size(), equalTo(10));
        MaxAgeCondition maxAgeCondition = (MaxAgeCondition) conditions.get(MaxAgeCondition.NAME);
        assertThat(maxAgeCondition.value.getMillis(), equalTo(TimeValue.timeValueHours(24 * 10).getMillis()));
        MaxDocsCondition maxDocsCondition = (MaxDocsCondition) conditions.get(MaxDocsCondition.NAME);
        assertThat(maxDocsCondition.value, equalTo(100L));
        MaxSizeCondition maxSizeCondition = (MaxSizeCondition) conditions.get(MaxSizeCondition.NAME);
        assertThat(maxSizeCondition.value.getBytes(), equalTo(ByteSizeUnit.GB.toBytes(45)));
        MaxPrimaryShardSizeCondition maxPrimaryShardSizeCondition = (MaxPrimaryShardSizeCondition) conditions.get(
            MaxPrimaryShardSizeCondition.NAME
        );
        assertThat(maxPrimaryShardSizeCondition.value.getBytes(), equalTo(ByteSizeUnit.GB.toBytes(55)));
        MaxPrimaryShardDocsCondition maxPrimaryShardDocsCondition = (MaxPrimaryShardDocsCondition) conditions.get(
            MaxPrimaryShardDocsCondition.NAME
        );
        assertThat(maxPrimaryShardDocsCondition.value, equalTo(10L));
        MinAgeCondition minAgeCondition = (MinAgeCondition) conditions.get(MinAgeCondition.NAME);
        assertThat(minAgeCondition.value.getMillis(), equalTo(TimeValue.timeValueHours(24 * 10).getMillis()));
        MinDocsCondition minDocsCondition = (MinDocsCondition) conditions.get(MinDocsCondition.NAME);
        assertThat(minDocsCondition.value, equalTo(100L));
        MinPrimaryShardSizeCondition minPrimaryShardSizeCondition = (MinPrimaryShardSizeCondition) conditions.get(
            MinPrimaryShardSizeCondition.NAME
        );
        assertThat(minPrimaryShardSizeCondition.value.getBytes(), equalTo(ByteSizeUnit.GB.toBytes(55)));
    }

    public void testParsingWithIndexSettings() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("conditions")
            .field("max_age", "10d")
            .field("max_docs", 100)
            .field("max_primary_shard_docs", 10)
            .endObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject("field1")
            .field("type", "string")
            .field("index", "not_analyzed")
            .endObject()
            .endObject()
            .endObject()
            .startObject("settings")
            .field("number_of_shards", 10)
            .endObject()
            .startObject("aliases")
            .startObject("alias1")
            .endObject()
            .endObject()
            .endObject();
        try (var parser = createParser(builder)) {
            request.fromXContent(parser);
        }
        Map<String, Condition<?>> conditions = request.getConditions().getConditions();
        assertThat(conditions.size(), equalTo(3));
        assertThat(request.getCreateIndexRequest().mappings(), containsString("not_analyzed"));
        assertThat(request.getCreateIndexRequest().aliases().size(), equalTo(1));
        assertThat(request.getCreateIndexRequest().settings().getAsInt("number_of_shards", 0), equalTo(10));
    }

    public void testTypelessMappingParsing() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        try (var parser = createParser(builder)) {
            request.fromXContent(parser);
        }
        CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
        String mapping = createIndexRequest.mappings();
        assertNotNull(mapping);

        Map<String, Object> parsedMapping = XContentHelper.convertToMap(new BytesArray(mapping), false, XContentType.JSON).v2();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) parsedMapping.get(MapperService.SINGLE_MAPPING_NAME);
        assertNotNull(properties);
        assertFalse(properties.isEmpty());
    }

    public void testSerialize() throws Exception {
        RolloverRequest originalRequest = new RolloverRequest("alias-index", "new-index-name");
        originalRequest.setConditions(
            RolloverConditions.newBuilder()
                .addMaxIndexDocsCondition(randomNonNegativeLong())
                .addMaxIndexAgeCondition(TimeValue.timeValueNanos(randomNonNegativeLong()))
                .addMaxIndexSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong()))
                .addMaxPrimaryShardSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong()))
                .addMaxPrimaryShardDocsCondition(randomNonNegativeLong())
                .addMinIndexDocsCondition(randomNonNegativeLong())
                .addMinIndexAgeCondition(TimeValue.timeValueNanos(randomNonNegativeLong()))
                .addMinIndexSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong()))
                .addMinPrimaryShardSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong()))
                .addMinPrimaryShardDocsCondition(randomNonNegativeLong())
                .build()
        );
        originalRequest.lazy(randomBoolean());
        originalRequest.setIndicesOptions(
            IndicesOptions.builder(originalRequest.indicesOptions()).selectorOptions(IndicesOptions.SelectorOptions.ALL_APPLICABLE).build()
        );

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalRequest.writeTo(out);
            BytesReference bytes = out.bytes();
            try (StreamInput in = new NamedWriteableAwareStreamInput(bytes.streamInput(), writeableRegistry)) {
                RolloverRequest cloneRequest = new RolloverRequest(in);
                assertThat(cloneRequest.getNewIndexName(), equalTo(originalRequest.getNewIndexName()));
                assertThat(cloneRequest.getRolloverTarget(), equalTo(originalRequest.getRolloverTarget()));
                assertThat(cloneRequest.isLazy(), equalTo(originalRequest.isLazy()));
                assertThat(cloneRequest.indicesOptions().selectorOptions(), equalTo(originalRequest.indicesOptions().selectorOptions()));
                for (Map.Entry<String, Condition<?>> entry : cloneRequest.getConditions().getConditions().entrySet()) {
                    Condition<?> condition = originalRequest.getConditions().getConditions().get(entry.getKey());
                    // here we compare the string representation as there is some information loss when serializing
                    // and de-serializing MaxAgeCondition/MinAgeCondition
                    assertEquals(condition.toString(), entry.getValue().toString());
                }
            }
        }
    }

    public void testUnknownFields() throws IOException {
        final RolloverRequest request = new RolloverRequest();
        XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        builder.startObject();
        {
            builder.startObject("conditions");
            builder.field("max_age", "10d");
            builder.endObject();
        }
        builder.endObject();
        BytesReference mutated = XContentTestUtils.insertRandomFields(xContentType, BytesReference.bytes(builder), null, random());
        expectThrows(XContentParseException.class, () -> {
            try (var parser = createParser(xContentType.xContent(), mutated)) {
                request.fromXContent(parser);
            }
        });
    }

    public void testValidation() {
        {
            RolloverRequest rolloverRequest = new RolloverRequest();
            assertNotNull(rolloverRequest.getCreateIndexRequest());
            ActionRequestValidationException validationException = rolloverRequest.validate();
            assertNotNull(validationException);
            assertEquals(1, validationException.validationErrors().size());
            assertEquals("rollover target is missing", validationException.validationErrors().get(0));
        }

        {
            RolloverRequest rolloverRequest = new RolloverRequest("alias-index", "new-index-name");
            rolloverRequest.setConditions(RolloverConditions.newBuilder().addMinIndexDocsCondition(1L).build());
            ActionRequestValidationException validationException = rolloverRequest.validate();
            assertNotNull(validationException);
            assertEquals(1, validationException.validationErrors().size());
            assertEquals(
                "at least one max_* rollover condition must be set when using min_* conditions",
                validationException.validationErrors().get(0)
            );
        }

        {
            RolloverRequest rolloverRequest = new RolloverRequest("alias-index", "new-index-name");
            if (randomBoolean()) {
                rolloverRequest.setConditions(
                    RolloverConditions.newBuilder()
                        .addMaxIndexAgeCondition(TimeValue.timeValueHours(1))
                        .addMinIndexDocsCondition(1L)
                        .build()
                );
            }
            ActionRequestValidationException validationException = rolloverRequest.validate();
            assertNull(validationException);
        }

        {
            RolloverRequest rolloverRequest = new RolloverRequest("alias-index", "new-index-name");
            rolloverRequest.setIndicesOptions(
                IndicesOptions.builder(rolloverRequest.indicesOptions())
                    .selectorOptions(IndicesOptions.SelectorOptions.ALL_APPLICABLE)
                    .build()
            );
            ActionRequestValidationException validationException = rolloverRequest.validate();
            assertNotNull(validationException);
            assertEquals(1, validationException.validationErrors().size());
            assertEquals(
                "rollover cannot be applied to both regular and failure indices at the same time",
                validationException.validationErrors().get(0)
            );
        }
    }
}
