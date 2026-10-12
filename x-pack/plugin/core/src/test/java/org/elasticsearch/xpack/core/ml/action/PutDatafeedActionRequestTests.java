/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction.Request;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_CONFIG_ESQL_INCOMPATIBLE_WITH_FIELD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
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

    public void testCloudCredentialSurvivesTransportRoundTrip() throws IOException {
        Request request = new Request(DatafeedConfigTests.createRandomizedDatafeedConfig(randomAlphaOfLength(10), datafeedId, 3600));
        CloudCredential credential = new CloudCredential(new SecureString("caller-uiam-token".toCharArray()));
        request.setCloudCredential(credential);
        TransportVersion version = DatafeedConfig.DATAFEED_CLOUD_INTERNAL_CREDENTIAL;

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(version);
        request.writeTo(out);

        StreamInput rawIn = out.bytes().streamInput();
        rawIn.setTransportVersion(version);
        try (StreamInput in = new NamedWriteableAwareStreamInput(rawIn, getNamedWriteableRegistry())) {
            Request deserialized = new Request(in);
            assertThat(deserialized.getCloudCredential(), notNullValue());
            assertThat(deserialized.getCloudCredential().value().toString(), equalTo("caller-uiam-token"));
        }
    }

    public void testCloudCredentialOmittedOnOlderTransportVersion() throws IOException {
        TransportVersion oldVersion = TransportVersion.fromName("datafeed_project_routing");
        assumeFalse(
            "Need a transport version older than datafeed_cloud_internal_credential",
            oldVersion.supports(DatafeedConfig.DATAFEED_CLOUD_INTERNAL_CREDENTIAL)
        );
        Request request = new Request(DatafeedConfigTests.createRandomizedDatafeedConfig(randomAlphaOfLength(10), datafeedId, 3600));
        request.setCloudCredential(new CloudCredential(new SecureString("caller-uiam-token".toCharArray())));

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        request.writeTo(out);

        StreamInput rawIn = out.bytes().streamInput();
        rawIn.setTransportVersion(oldVersion);
        try (StreamInput in = new NamedWriteableAwareStreamInput(rawIn, getNamedWriteableRegistry())) {
            Request deserialized = new Request(in);
            assertThat(deserialized.getCloudCredential(), nullValue());
        }
    }

    public void testParseRequestDslDatafeedShouldKeepRestDefaultIndicesOptions() throws IOException {
        String json = """
            {"job_id": "job1", "indices": ["index-1"]}
            """;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            Request request = Request.parseRequest("datafeed1", SearchRequest.DEFAULT_INDICES_OPTIONS, parser);
            assertThat(request.getDatafeed().getIndicesOptions(), equalTo(SearchRequest.DEFAULT_INDICES_OPTIONS));
        }
    }

    public void testParseRequestDslDatafeedWithExplicitIndicesOptionsShouldKeepThem() throws IOException {
        String json = """
            {"job_id": "job1", "indices": ["index-1"], "indices_options": {"expand_wildcards": ["all"]}}
            """;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            Request request = Request.parseRequest("datafeed1", SearchRequest.DEFAULT_INDICES_OPTIONS, parser);
            assertThat(request.getDatafeed().getIndicesOptions(), notNullValue());
            assertThat(request.getDatafeed().getIndicesOptions().equals(SearchRequest.DEFAULT_INDICES_OPTIONS), equalTo(false));
        }
    }

    public void testParseRequestEsqlDatafeedShouldHaveNoIndicesOptions() throws IOException {
        // The REST handler always passes a non-null default; it must not be applied to ES|QL datafeeds.
        String json = """
            {"job_id": "job1", "esql_query": "FROM logs", "source_time_field": "@timestamp", "grouping_interval": "1h"}
            """;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            Request request = Request.parseRequest("datafeed1", SearchRequest.DEFAULT_INDICES_OPTIONS, parser);
            assertThat(request.getDatafeed().getEsqlQuery(), equalTo("FROM logs"));
            assertThat(request.getDatafeed().getIndicesOptions(), nullValue());
        }
    }

    public void testParseRequestEsqlDatafeedWithExplicitIndicesOptionsShouldBeRejected() throws IOException {
        String json = """
            {
              "job_id": "job1",
              "esql_query": "FROM logs",
              "source_time_field": "@timestamp",
              "grouping_interval": "1h",
              "indices_options": {"expand_wildcards": ["all"]}
            }
            """;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            ElasticsearchStatusException e = expectThrows(
                ElasticsearchStatusException.class,
                () -> Request.parseRequest("datafeed1", SearchRequest.DEFAULT_INDICES_OPTIONS, parser)
            );
            assertThat(e.getMessage(), equalTo(Messages.getMessage(DATAFEED_CONFIG_ESQL_INCOMPATIBLE_WITH_FIELD, "indices_options")));
        }
    }
}
