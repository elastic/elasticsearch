/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction.Request;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.test.BWCVersions.DEFAULT_BWC_VERSIONS;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class PreviewTransformActionRequestTests extends AbstractSerializingTransformTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) throws IOException {
        return Request.fromXContent(parser, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, false);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(randomTransformConfig(), randomTimeValue(), randomBoolean());
    }

    private static TransformConfig randomTransformConfig() {
        return new TransformConfig(
            "transform-preview",
            randomSourceConfig(),
            new DestConfig("unused-transform-preview-index", null, null),
            null,
            randomBoolean() ? TransformConfigTests.randomSyncConfig() : null,
            null,
            PivotConfigTests.randomPivotConfig(),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    @Override
    protected Request createXContextTestInstance(XContentType xContentType) {
        return new Request(randomTransformConfig(), AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, false);
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return randomBoolean()
            ? new Request(
                randomValueOtherThan(instance.getConfig(), PreviewTransformActionRequestTests::randomTransformConfig),
                instance.ackTimeout(),
                instance.previewAsIndexRequest()
            )
            : new Request(instance.getConfig(), instance.ackTimeout(), instance.previewAsIndexRequest() == false);
    }

    public void testAsIndexRequestIsNotBackwardsCompatible() throws IOException {
        var unsupportedVersions = DEFAULT_BWC_VERSIONS.stream()
            .filter(Predicate.not(version -> version.supports(Request.PREVIEW_AS_INDEX_REQUEST)))
            .toList();
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            var testInstance = createTestInstance();
            for (var unsupportedVersion : unsupportedVersions) {
                if (testInstance.previewAsIndexRequest()) {
                    var statusException = assertThrows(
                        ElasticsearchStatusException.class,
                        () -> copyWriteable(testInstance, getNamedWriteableRegistry(), instanceReader(), unsupportedVersion)
                    );
                    assertThat(statusException.status(), equalTo(RestStatus.BAD_REQUEST));
                    assertThat(
                        statusException.getMessage(),
                        equalTo(
                            "Cannot send a _preview request with as_index_request to an outdated node. "
                                + "Please upgrade the node to 9.3.0+ and try again."
                        )
                    );
                } else {
                    var deserializedInstance = copyWriteable(
                        testInstance,
                        getNamedWriteableRegistry(),
                        instanceReader(),
                        unsupportedVersion
                    );
                    assertNotSame(unsupportedVersion.toString(), deserializedInstance, testInstance);
                    assertEquals(unsupportedVersion.toString(), deserializedInstance, testInstance);
                    assertEquals(unsupportedVersion.toString(), deserializedInstance.hashCode(), testInstance.hashCode());
                }
            }
        }
    }

    public void testParsingOverwritesIdField() throws IOException {
        testParsingOverwrites("", """
            "dest": {"index": "bar","pipeline": "baz"},""", "transform-preview", "bar", "baz");
    }

    public void testParsingOverwritesDestField() throws IOException {
        testParsingOverwrites("\"id\": \"bar\",", "", "bar", "unused-transform-preview-index", null);
    }

    public void testParsingOverwritesIdAndDestIndexFields() throws IOException {
        testParsingOverwrites("", """
            "dest": {"pipeline": "baz"},""", "transform-preview", "unused-transform-preview-index", "baz");
    }

    public void testParsingOverwritesIdAndDestFields() throws IOException {
        testParsingOverwrites("", "", "transform-preview", "unused-transform-preview-index", null);
    }

    private void testParsingOverwrites(
        String transformIdJson,
        String destConfigJson,
        String expectedTransformId,
        String expectedDestIndex,
        String expectedDestPipeline
    ) throws IOException {
        BytesArray json = new BytesArray(Strings.format("""
            {
              %s
              "source": {
                "index": "foo",
                "query": {
                  "match_all": {}
                }
              },
              %s
              "pivot": {
                "group_by": {
                  "destination-field2": {
                    "terms": {
                      "field": "term-field"
                    }
                  }
                },
                "aggs": {
                  "avg_response": {
                    "avg": {
                      "field": "responsetime"
                    }
                  }
                }
              }
            }""", transformIdJson, destConfigJson));

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry())
                    .withDeprecationHandler(DeprecationHandler.THROW_UNSUPPORTED_OPERATION),
                json.streamInput()
            )
        ) {

            Request request = Request.fromXContent(parser, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, false);
            assertThat(request.getConfig().getId(), is(equalTo(expectedTransformId)));
            assertThat(request.getConfig().getDestination().getIndex(), is(equalTo(expectedDestIndex)));
            assertThat(request.getConfig().getDestination().getPipeline(), is(equalTo(expectedDestPipeline)));
        }
    }

    public void testCreateTask() {
        Request request = createTestInstance();
        Task task = request.createTask(123, "type", "action", TaskId.EMPTY_TASK_ID, Map.of());
        assertThat(task, is(instanceOf(CancellableTask.class)));
        assertThat(task.getDescription(), is(equalTo("preview_transform[transform-preview]")));
    }
}
