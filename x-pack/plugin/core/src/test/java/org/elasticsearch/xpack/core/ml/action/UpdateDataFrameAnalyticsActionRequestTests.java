/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.UpdateDataFrameAnalyticsAction.Request;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigUpdate;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigUpdateTests;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests.randomValidId;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class UpdateDataFrameAnalyticsActionRequestTests extends AbstractSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(DataFrameAnalyticsConfigUpdateTests.randomUpdate(randomValidId()));
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(null, parser);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    public void testParseRequest() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{}")) {
            Request request = Request.parseRequest("id-from-param", parser);
            assertThat(request.getUpdate(), is(equalTo(new DataFrameAnalyticsConfigUpdate.Builder("id-from-param").build())));
        }
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"id\": \"id-from-body\"}")) {
            Request request = Request.parseRequest(null, parser);
            assertThat(request.getUpdate(), is(equalTo(new DataFrameAnalyticsConfigUpdate.Builder("id-from-body").build())));
        }
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"id\": \"same-id\"}")) {
            Request request = Request.parseRequest("same-id", parser);
            assertThat(request.getUpdate(), is(equalTo(new DataFrameAnalyticsConfigUpdate.Builder("same-id").build())));
        }
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"id\": \"id-from-body\"}")) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Request.parseRequest("id-from-param", parser));
            assertThat(e.getMessage(), startsWith("Inconsistent id"));
        }
    }

    public void testDefaultTimeout() {
        assertThat(createTestInstance().timeout(), is(notNullValue()));
    }
}
