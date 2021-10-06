/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction.Request;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;

import java.io.IOException;

import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PreviewTransformActionRequestTests extends AbstractSerializingTransformTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) throws IOException {
        return Request.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createTestInstance() {
        TransformConfig config = new TransformConfig(
            "transform-preview",
            randomSourceConfig(),
            new DestConfig("unused-transform-preview-index", null),
            null,
            randomBoolean() ? TransformConfigTests.randomSyncConfig() : null,
            null,
            PivotConfigTests.randomPivotConfig(),
            null,
            null,
            null,
            null,
            null,
            null
        );
        return new Request(config);
    }

    public void testParsingOverwritesIdField() throws IOException {
        testParsingOverwrites(
            "",
            "\"dest\": {"
                + "\"index\": \"bar\","
                + "\"pipeline\": \"baz\""
                + "},",
            "transform-preview",
            "bar",
            "baz"
        );
    }

    public void testParsingOverwritesDestField() throws IOException {
        testParsingOverwrites(
            "\"id\": \"bar\",",
            "",
            "bar",
            "unused-transform-preview-index",
            null
        );
    }

    public void testParsingOverwritesIdAndDestIndexFields() throws IOException {
        testParsingOverwrites(
            "",
            "\"dest\": {"
                + "\"pipeline\": \"baz\""
            + "},",
            "transform-preview",
            "unused-transform-preview-index",
            "baz"
        );
    }

    public void testParsingOverwritesIdAndDestFields() throws IOException {
        testParsingOverwrites(
            "",
            "",
            "transform-preview",
            "unused-transform-preview-index",
            null
        );
    }

    private void testParsingOverwrites(
        String transformIdJson,
        String destConfigJson,
        String expectedTransformId,
        String expectedDestIndex,
        String expectedDestPipeline
    ) throws IOException {
        BytesArray json = new BytesArray(
            "{ "
                + transformIdJson
                + "\"source\": {"
                + "   \"index\": \"foo\", "
                + "   \"query\": {\"match_all\": {}}},"
                + destConfigJson
                + "\"pivot\": {"
                + "\"group_by\": {\"destination-field2\": {\"terms\": {\"field\": \"term-field\"}}},"
                + "\"aggs\": {\"avg_response\": {\"avg\": {\"field\": \"responsetime\"}}}"
                + "}"
                + "}"
        );

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json.streamInput()
            )
        ) {

            Request request = Request.fromXContent(parser);
            assertThat(request.getConfig().getId(), is(equalTo(expectedTransformId)));
            assertThat(request.getConfig().getDestination().getIndex(), is(equalTo(expectedDestIndex)));
            assertThat(request.getConfig().getDestination().getPipeline(), is(equalTo(expectedDestPipeline)));
        }
    }
}
