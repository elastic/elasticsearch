/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ClusterStateSecretsMetadataTests extends AbstractNamedWriteableTestCase<ClusterStateSecretsMetadata> {

    public void testToXContentChunkedSuccess() throws Exception {
        ClusterStateSecretsMetadata metadata = ClusterStateSecretsMetadata.createSuccessful(1L);
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        metadata.toXContentChunked(EMPTY_PARAMS).forEachRemaining(xcontent -> {
            try {
                xcontent.toXContent(builder, EMPTY_PARAMS);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                fail(e.getMessage());
            }
        });
        builder.endObject();
        Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        assertThat(xContentMap.size(), equalTo(2));
        assertThat(xContentMap.get("success"), equalTo(true));
        assertThat(xContentMap.get("version"), equalTo(1));
        assertThat(xContentMap.get("stack_trace"), nullValue());
    }

    public void testToXContentChunkedFailure() throws Exception {
        ClusterStateSecretsMetadata metadata = ClusterStateSecretsMetadata.createError(-1L, List.of("line1\n", "line2\n"));
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        metadata.toXContentChunked(EMPTY_PARAMS).forEachRemaining(xcontent -> {
            try {
                xcontent.toXContent(builder, EMPTY_PARAMS);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                fail(e.getMessage());
            }
        });
        builder.endObject();
        Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        assertThat(xContentMap.size(), equalTo(3));
        assertThat(xContentMap.get("success"), equalTo(false));
        assertThat(xContentMap.get("version"), equalTo(-1));
        assertThat(xContentMap.get("stack_trace"), equalTo(List.of("line1\n", "line2\n")));
    }

    public void testSerializeSuccess() throws Exception {
        ClusterStateSecretsMetadata metadata = ClusterStateSecretsMetadata.createSuccessful(1L);
        final BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);
        final ClusterStateSecretsMetadata fromStream = new ClusterStateSecretsMetadata(out.bytes().streamInput());

        assertThat(fromStream, equalTo(metadata));
    }

    public void testSerializeError() throws Exception {
        ClusterStateSecretsMetadata metadata = ClusterStateSecretsMetadata.createError(-1L, List.of("line1\n", "line2\n"));
        final BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);
        final ClusterStateSecretsMetadata fromStream = new ClusterStateSecretsMetadata(out.bytes().streamInput());

        assertThat(fromStream, equalTo(metadata));
    }

    @Override
    protected ClusterStateSecretsMetadata createTestInstance() {
        if (randomBoolean()) {
            return ClusterStateSecretsMetadata.createSuccessful(randomLong());
        }
        return ClusterStateSecretsMetadata.createError(randomLong(), randomList(0, 10, () -> randomAlphaOfLength(10)));
    }

    @Override
    protected ClusterStateSecretsMetadata mutateInstance(ClusterStateSecretsMetadata instance) throws IOException {
        if (randomBoolean()) {
            // change success to failure
            if (instance.isSuccess()) {
                return ClusterStateSecretsMetadata.createError(instance.getVersion(), randomList(0, 10, () -> randomAlphaOfLength(10)));
            }
            return ClusterStateSecretsMetadata.createSuccessful(instance.getVersion());
        }
        if (instance.isSuccess()) {
            return ClusterStateSecretsMetadata.createSuccessful(randomValueOtherThan(instance.getVersion(), ESTestCase::randomLong));
        }
        return ClusterStateSecretsMetadata.createError(
            randomValueOtherThan(instance.getVersion(), ESTestCase::randomLong),
            instance.getErrorStackTrace()
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    ClusterStateSecretsMetadata.class,
                    ClusterStateSecretsMetadata.TYPE,
                    ClusterStateSecretsMetadata::new
                )
            )
        );
    }

    @Override
    protected Class<ClusterStateSecretsMetadata> categoryClass() {
        return ClusterStateSecretsMetadata.class;
    }
}
