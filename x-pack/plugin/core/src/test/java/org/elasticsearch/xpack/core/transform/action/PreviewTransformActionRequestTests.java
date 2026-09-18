/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction.Request;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformParsingContext;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.test.BWCVersions.DEFAULT_BWC_VERSIONS;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.elasticsearch.xpack.core.transform.transforms.TransformConfig.TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PreviewTransformActionRequestTests extends AbstractSerializingTransformTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) throws IOException {
        return Request.fromXContent(parser, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, false, new TransformParsingContext(false));
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomTransformConfig(), randomTimeValue(), randomBoolean());
        // Randomly include a cloud credential so the wire path with the optional field is exercised
        // by the inherited round-trip test even though the field is excluded from equals/hashCode.
        request.setCloudCredential(randomBoolean() ? randomCloudCredential() : null);
        return request;
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
        Request mutated = randomBoolean()
            ? new Request(
                randomValueOtherThan(instance.getConfig(), PreviewTransformActionRequestTests::randomTransformConfig),
                instance.ackTimeout(),
                instance.previewAsIndexRequest()
            )
            : new Request(instance.getConfig(), instance.ackTimeout(), instance.previewAsIndexRequest() == false);
        mutated.setCloudCredential(instance.getCloudCredential());
        return mutated;
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, TransportVersion version) {
        // cloudCredential is excluded from Request.equals so it passes through unchanged here; the explicit
        // drop semantics are asserted by testCloudCredentialDroppedWhenWireVersionTooOld.
        Request mutated = new Request(
            TransformConfigTests.mutateForVersion(instance.getConfig(), version),
            instance.ackTimeout(),
            instance.previewAsIndexRequest()
        );
        mutated.setCloudCredential(instance.getCloudCredential());
        return mutated;
    }

    // versions before PREVIEW_AS_INDEX_REQUEST throw an exception - those are tested in testAsIndexRequestIsNotBackwardsCompatible
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(Request.PREVIEW_AS_INDEX_REQUEST)).toList();
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

            Request request = Request.fromXContent(
                parser,
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                false,
                new TransformParsingContext(false)
            );
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

    public void testCloudCredentialRoundTripPreservesValue() throws IOException {
        String secret = randomAlphaOfLengthBetween(8, 32);
        Request original = new Request(randomTransformConfig(), randomTimeValue(), randomBoolean());
        original.setCloudCredential(new CloudCredential(new SecureString(secret.toCharArray())));

        Request copy = copyWriteable(original, getNamedWriteableRegistry(), instanceReader());
        try {
            assertThat(copy.getCloudCredential(), is(notNullValue()));
            assertThat(copy.getCloudCredential().value().toString(), is(secret));
        } finally {
            copy.close();
        }
    }

    public void testCloudCredentialDroppedWhenWireVersionTooOld() throws IOException {
        Request original = new Request(randomTransformConfig(), randomTimeValue(), false);
        original.setCloudCredential(randomCloudCredential());

        var olderVersion = TransportVersionUtils.randomVersionNotSupporting(TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST);
        Request copy = copyWriteable(original, getNamedWriteableRegistry(), instanceReader(), olderVersion);
        try {
            // Older receivers can't decode the new optional field, so it must round-trip as null.
            assertThat(copy.getCloudCredential(), is(nullValue()));
        } finally {
            copy.close();
        }
    }

    public void testRequestCloseIsIdempotentWithCredential() {
        // Both the sender's and receiver's listeners may fire close() on the same Request instance
        // (local dispatch reuses the same instance). The contract we rely on is that a second close()
        // is a safe no-op so we never need to coordinate which side closes the credential.
        var credential = randomCloudCredential();
        var request = new Request(randomTransformConfig(), randomTimeValue(), false);
        request.setCloudCredential(credential);

        request.close();
        // SecureString.length() throws once close() has zeroed the underlying char array.
        expectThrows(IllegalStateException.class, () -> credential.value().length());

        // Second close must not throw.
        request.close();
    }

    public void testSetCloudCredentialReturnsPreviousCredential() {
        // Overwriting hands ownership of the previous credential back to the caller: it is
        // returned still open (not zeroed) so the caller decides when to close it.
        var first = randomCloudCredential();
        var second = randomCloudCredential();
        var request = new Request(randomTransformConfig(), randomTimeValue(), false);
        assertThat(request.setCloudCredential(first), is(nullValue()));

        var previous = request.setCloudCredential(second);
        assertThat(previous, is(first));
        assertThat(previous.value().length() > 0, is(true));
        assertThat(request.getCloudCredential(), is(second));

        previous.close();
        request.close();
    }

    public void testSetCloudCredentialSelfAssignmentReturnsNull() {
        // Re-setting the credential the request already holds returns null so a caller closing
        // the returned value can never zero the credential the request still carries.
        var credential = randomCloudCredential();
        var request = new Request(randomTransformConfig(), randomTimeValue(), false);
        request.setCloudCredential(credential);

        assertThat(request.setCloudCredential(credential), is(nullValue()));
        assertThat(credential.value().length() > 0, is(true));

        request.close();
    }

    public void testRequestCloseIsIdempotentWithoutCredential() {
        // Non-UIAM callers leave the credential null. The same close-twice path must be a no-op.
        var request = new Request(randomTransformConfig(), randomTimeValue(), false);

        request.close();
        request.close();
    }

    private static CloudCredential randomCloudCredential() {
        return new CloudCredential(new SecureString(randomAlphaOfLengthBetween(8, 32).toCharArray()));
    }
}
