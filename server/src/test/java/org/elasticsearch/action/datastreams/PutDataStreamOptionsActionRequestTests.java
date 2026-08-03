/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetricsTests;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.DataStreamOptionsTests;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class PutDataStreamOptionsActionRequestTests extends AbstractWireSerializingTestCase<PutDataStreamOptionsAction.Request> {

    @Override
    protected Writeable.Reader<PutDataStreamOptionsAction.Request> instanceReader() {
        return PutDataStreamOptionsAction.Request::new;
    }

    @Override
    protected PutDataStreamOptionsAction.Request createTestInstance() {
        return new PutDataStreamOptionsAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            randomNames(),
            randomNonEmptyDataStreamOptions()
        );
    }

    @Override
    protected PutDataStreamOptionsAction.Request mutateInstance(PutDataStreamOptionsAction.Request instance) throws IOException {
        return randomBoolean()
            ? new PutDataStreamOptionsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                randomValueOtherThanMany(names -> Arrays.equals(names, instance.getNames()), this::randomNames),
                instance.getOptions()
            )
            : new PutDataStreamOptionsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                instance.getNames(),
                randomValueOtherThan(instance.getOptions(), PutDataStreamOptionsActionRequestTests::randomNonEmptyDataStreamOptions)
            );
    }

    public void testValidateDerivedMetricsOnlyRequest() {
        PutDataStreamOptionsAction.Request request = new PutDataStreamOptionsAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            new String[] { "logs-my_app-default" },
            new DataStreamOptions(null, DataStreamDerivedMetricsTests.randomDerivedMetrics())
        );

        assertThat(request.validate(), nullValue());
    }

    public void testValidateEmptyRequest() {
        PutDataStreamOptionsAction.Request request = new PutDataStreamOptionsAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            new String[] { "logs-my_app-default" },
            DataStreamOptions.EMPTY
        );

        ActionRequestValidationException e = request.validate();
        assertThat(e.validationErrors().size(), equalTo(1));
        assertThat(e.validationErrors().get(0), containsString("At least one option needs to be provided"));
    }

    public void testParseDerivedMetricsOnlyRequest() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "derived_metrics": {
                "default_interval": "10s",
                "destinations": {
                  "1m": { "lifecycle": { "data_retention": "90d" } }
                },
                "metrics": [
                  {
                    "name": "http.requests",
                    "type": "counter",
                    "dimensions": ["service.name"],
                    "interval": "1m"
                  }
                ]
              }
            }
            """)) {
            PutDataStreamOptionsAction.Request request = PutDataStreamOptionsAction.Request.parseRequest(
                parser,
                options -> new PutDataStreamOptionsAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    new String[] { "logs-my_app-default" },
                    options
                )
            );

            assertThat(request.validate(), nullValue());
            assertThat(request.getOptions().failureStore(), nullValue());
            assertThat(request.getOptions().derivedMetrics().builtin(), equalTo(List.of("ingest.*")));
            assertThat(request.getOptions().derivedMetrics().metrics().get(0).name(), equalTo("http.requests"));
            assertThat(request.getOptions().derivedMetrics().defaultInterval(), equalTo(TimeValue.timeValueSeconds(10)));
            // the metric overrides the interval, so it is written to the 1m destination declared above
            assertThat(request.getOptions().derivedMetrics().metrics().get(0).interval(), equalTo(TimeValue.timeValueMinutes(1)));
        }
    }

    private static DataStreamOptions randomNonEmptyDataStreamOptions() {
        return randomValueOtherThan(DataStreamOptions.EMPTY, DataStreamOptionsTests::randomDataStreamOptions);
    }

    private String[] randomNames() {
        return randomArray(1, 5, String[]::new, () -> randomAlphaOfLength(8));
    }
}
