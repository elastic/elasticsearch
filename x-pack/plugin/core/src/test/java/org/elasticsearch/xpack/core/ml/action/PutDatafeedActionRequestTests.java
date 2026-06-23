/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction.Request;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class PutDatafeedActionRequestTests extends AbstractXContentSerializingTestCase<Request> {

    private String datafeedId;

    @Before
    public void setUpDatafeedId() {
        datafeedId = DatafeedConfigTests.randomValidDatafeedId();
    }

    @Override
    protected Request createTestInstance() {
        return new Request(DatafeedConfigTests.createRandomizedDatafeedConfig(randomAlphaOfLength(10), datafeedId, 3600));
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(datafeedId, SearchRequest.DEFAULT_INDICES_OPTIONS, parser);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    /**
     * Verifies that an ESQL datafeed request succeeds and has null indicesOptions
     * even when the caller passes a non-null default (as RestPutDatafeedAction does).
     * The default should be silently ignored for ESQL datafeeds.
     */
    public void testParseRequest_GivenEsqlQueryWithNoIndicesOptions_DoesNotInjectDefault() throws IOException {
        String body = """
            {
              "job_id": "test_job",
              "indices": ["test"],
              "esql_query": "FROM test | KEEP @timestamp"
            }
            """;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), body)) {
            // Simulate what RestPutDatafeedAction does: always pass a non-null default
            Request request = Request.parseRequest("test_datafeed", SearchRequest.DEFAULT_INDICES_OPTIONS, parser);
            assertThat(request.getDatafeed().getIndicesOptions(), nullValue());
        }
    }

    /**
     * Verifies that an ESQL datafeed request still rejects an explicit indices_options
     * supplied in the request body.
     */
    public void testParseRequest_GivenEsqlQueryWithExplicitIndicesOptions_Throws() throws IOException {
        String body = """
            {
              "job_id": "test_job",
              "indices": ["test"],
              "esql_query": "FROM test | KEEP @timestamp",
              "indices_options": { "expand_wildcards": "open" }
            }
            """;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), body)) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> Request.parseRequest("test_datafeed", SearchRequest.DEFAULT_INDICES_OPTIONS, parser)
            );
            assertThat(e.getMessage(), containsString(Messages.DATAFEED_CONFIG_ESQL_INCOMPATIBLE_WITH_INDICES_OPTIONS));
        }
    }
}
