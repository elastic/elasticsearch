/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction.Response;
import org.elasticsearch.xpack.core.transform.action.compat.UpdateTransformActionPre78;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class UpdateTransformsActionResponseTests extends AbstractSerializingTransformTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return new Response(TransformConfigTests.randomTransformConfigWithoutHeaders());
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::fromStreamWithBWC;
    }

    @Override
    protected Response doParseInstance(XContentParser parser) throws IOException {
        return new Response(TransformConfig.fromXContent(parser, null, false));
    }

    public void testBWCPre78() throws IOException {
        // latest has been added in 7.11, so we only need to test pivot
        Response newResponse = new Response(
            TransformConfigTests.randomTransformConfigWithoutHeaders(
                randomAlphaOfLengthBetween(1, 10),
                PivotConfigTests.randomPivotConfig(),
                null
            )
        );
        UpdateTransformActionPre78.Response oldResponse = writeAndReadBWCObject(
            newResponse,
            getNamedWriteableRegistry(),
            (out, value) -> value.writeTo(out),
            UpdateTransformActionPre78.Response::new,
            Version.V_7_7_0
        );
        assertEquals(newResponse.getConfig().getDescription(), oldResponse.getConfig().getDescription());
        assertEquals(newResponse.getConfig().getId(), oldResponse.getConfig().getId());
        assertEquals(newResponse.getConfig().getCreateTime(), oldResponse.getConfig().getCreateTime());
        assertEquals(newResponse.getConfig().getDestination(), oldResponse.getConfig().getDestination());
        assertEquals(newResponse.getConfig().getFrequency(), oldResponse.getConfig().getFrequency());
        assertEquals(
            newResponse.getConfig().getPivotConfig().getGroupConfig().getGroups().keySet(),
            oldResponse.getConfig().getPivotConfig().getGroupConfig().getGroups().keySet()
        );
        for (Map.Entry<String, SingleGroupSource> oldResponseGroupEntry : oldResponse.getConfig()
            .getPivotConfig()
            .getGroupConfig()
            .getGroups()
            .entrySet()) {
            SingleGroupSource oldResponseGroup = oldResponseGroupEntry.getValue();
            SingleGroupSource newResponseGroup = newResponse.getConfig()
                .getPivotConfig()
                .getGroupConfig()
                .getGroups()
                .get(oldResponseGroupEntry.getKey());
            assertThat(oldResponseGroup.getField(), is(equalTo(newResponseGroup.getField())));
            assertThat(oldResponseGroup.getScriptConfig(), is(equalTo(newResponseGroup.getScriptConfig())));
            // missing_bucket was added in 7.10 so it is always false after deserializing from 7.7
            assertThat(oldResponseGroup.getMissingBucket(), is(false));
        }
        assertEquals(
            newResponse.getConfig().getPivotConfig().getAggregationConfig(),
            oldResponse.getConfig().getPivotConfig().getAggregationConfig()
        );
        assertEquals(
            newResponse.getConfig().getPivotConfig().getMaxPageSearchSize(),
            oldResponse.getConfig().getPivotConfig().getMaxPageSearchSize()
        );
        if (newResponse.getConfig().getSource() != null) {
            assertThat(newResponse.getConfig().getSource().getIndex(), is(equalTo(newResponse.getConfig().getSource().getIndex())));
            assertThat(
                newResponse.getConfig().getSource().getQueryConfig(),
                is(equalTo(newResponse.getConfig().getSource().getQueryConfig()))
            );
            // runtime_mappings was added in 7.12 so it is always empty after deserializing from 7.7
            assertThat(oldResponse.getConfig().getSource().getRuntimeMappings(), is(anEmptyMap()));
        }
        assertEquals(newResponse.getConfig().getSyncConfig(), oldResponse.getConfig().getSyncConfig());
        assertEquals(newResponse.getConfig().getVersion(), oldResponse.getConfig().getVersion());

        //
        Response newRequestFromOld = writeAndReadBWCObject(
            oldResponse,
            getNamedWriteableRegistry(),
            (out, value) -> value.writeTo(out),
            Response::fromStreamWithBWC,
            Version.V_7_7_0
        );

        assertEquals(newResponse.getConfig().getDescription(), newRequestFromOld.getConfig().getDescription());
        assertEquals(newResponse.getConfig().getId(), newRequestFromOld.getConfig().getId());
        assertEquals(newResponse.getConfig().getCreateTime(), newRequestFromOld.getConfig().getCreateTime());
        assertEquals(newResponse.getConfig().getDestination(), newRequestFromOld.getConfig().getDestination());
        assertEquals(newResponse.getConfig().getFrequency(), newRequestFromOld.getConfig().getFrequency());
        assertEquals(
            newResponse.getConfig().getPivotConfig().getGroupConfig().getGroups().keySet(),
            newRequestFromOld.getConfig().getPivotConfig().getGroupConfig().getGroups().keySet()
        );
        for (Map.Entry<String, SingleGroupSource> newRequestFromOldGroupEntry : newRequestFromOld.getConfig()
            .getPivotConfig()
            .getGroupConfig()
            .getGroups()
            .entrySet()) {
            SingleGroupSource newRequestFromOldGroup = newRequestFromOldGroupEntry.getValue();
            SingleGroupSource newResponseGroup = newResponse.getConfig()
                .getPivotConfig()
                .getGroupConfig()
                .getGroups()
                .get(newRequestFromOldGroupEntry.getKey());
            assertThat(newRequestFromOldGroup.getField(), is(equalTo(newResponseGroup.getField())));
            assertThat(newRequestFromOldGroup.getScriptConfig(), is(equalTo(newResponseGroup.getScriptConfig())));
            // missing_bucket was added in 7.10 so it is always false after deserializing from 7.7
            assertThat(newRequestFromOldGroup.getMissingBucket(), is(false));
        }
        assertEquals(
            newResponse.getConfig().getPivotConfig().getAggregationConfig(),
            newRequestFromOld.getConfig().getPivotConfig().getAggregationConfig()
        );
        assertEquals(
            newResponse.getConfig().getPivotConfig().getMaxPageSearchSize(),
            newRequestFromOld.getConfig().getPivotConfig().getMaxPageSearchSize()
        );
        if (newResponse.getConfig().getSource() != null) {
            assertThat(newRequestFromOld.getConfig().getSource().getIndex(), is(equalTo(newResponse.getConfig().getSource().getIndex())));
            assertThat(
                newRequestFromOld.getConfig().getSource().getQueryConfig(),
                is(equalTo(newResponse.getConfig().getSource().getQueryConfig()))
            );
            // runtime_mappings was added in 7.12 so it is always empty after deserializing from 7.7
            assertThat(newRequestFromOld.getConfig().getSource().getRuntimeMappings(), is(anEmptyMap()));
        }
        assertEquals(newResponse.getConfig().getSyncConfig(), newRequestFromOld.getConfig().getSyncConfig());
        assertEquals(newResponse.getConfig().getVersion(), newRequestFromOld.getConfig().getVersion());
    }
}
