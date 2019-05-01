/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.dataframe.action.PreviewDataFrameTransformAction.Request;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DestConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.PivotConfigTests;
import org.junit.Before;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.core.dataframe.transforms.SourceConfigTests.randomSourceConfig;

public class PreviewDataFrameTransformActionRequestTests extends AbstractSerializingTestCase<Request> {

    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry namedXContentRegistry;

    @Before
    public void registerAggregationNamedObjects() {
        // register aggregations as NamedWriteable
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        namedXContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

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
        DataFrameTransformConfig config = new DataFrameTransformConfig("transform-preview", randomSourceConfig(),
                new DestConfig("unused-transform-preview-index"), null, PivotConfigTests.randomPivotConfig(), null);
        return new Request(config);
    }

    public void testParsingOverwritesIdAndDestFields() throws IOException {
        // id & dest fields will be set by the parser
        BytesArray json = new BytesArray(
                "{ " +
                    "\"source\":{" +
                    "   \"index\":\"foo\", " +
                    "   \"query\": {\"match_all\": {}}}," +
                    "\"pivot\": {" +
                        "\"group_by\": {\"destination-field2\": {\"terms\": {\"field\": \"term-field\"}}}," +
                        "\"aggs\": {\"avg_response\": {\"avg\": {\"field\": \"responsetime\"}}}" +
                    "}" +
                "}");

        try (XContentParser parser = JsonXContent.jsonXContent
                .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json.streamInput())) {

            Request request = Request.fromXContent(parser);
            assertEquals("transform-preview", request.getConfig().getId());
            assertEquals("unused-transform-preview-index", request.getConfig().getDestination().getIndex());
        }
    }
}
