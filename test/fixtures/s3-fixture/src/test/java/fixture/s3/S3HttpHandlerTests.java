/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.s3;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.oneOf;

public class S3HttpHandlerTests extends ESTestCase {

    public void testRejectsBadUri() {
        assertEquals(
            RestStatus.INTERNAL_SERVER_ERROR,
            handleRequest(
                new S3HttpHandler("bucket", "path", S3ConsistencyModel.randomConsistencyModel()),
                randomFrom("GET", "PUT", "POST", "DELETE", "HEAD"),
                "/not-in-bucket"
            ).status()
        );
    }

    private static void assertListObjectsResponse(
        S3HttpHandler handler,
        @Nullable String prefix,
        @Nullable String delimiter,
        String expectedResponse
    ) {
        final var queryParts = new ArrayList<String>(3);
        if (prefix != null) {
            queryParts.add("prefix=" + prefix);
        }
        if (delimiter != null) {
            queryParts.add("delimiter=" + delimiter);
        }
        if (randomBoolean()) {
            // test both ListObjects and ListObjectsV2 - they only differ in terms of pagination but S3HttpHandler doesn't do that
            queryParts.add("list-type=2");
        }
        Randomness.shuffle(queryParts);

        final var requestUri = "/bucket" + randomFrom("", "/") + (queryParts.isEmpty() ? "" : "?" + String.join("&", queryParts));
        assertEquals("GET " + requestUri, new TestHttpResponse(RestStatus.OK, expectedResponse), handleRequest(handler, "GET", requestUri));
    }

    public void testSimpleObjectOperations() {
        final var handler = new S3HttpHandler("bucket", "path", S3ConsistencyModel.randomConsistencyModel());

        assertEquals(RestStatus.NOT_FOUND, handleRequest(handler, "GET", "/bucket/path/blob").status());

        assertListObjectsResponse(handler, "", null, """
            <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix></Prefix><IsTruncated>false</IsTruncated>\
            </ListBucketResult>""");

        final var body = new BytesArray(randomAlphaOfLength(50).getBytes(StandardCharsets.UTF_8));
        assertEquals(RestStatus.OK, handleRequest(handler, "PUT", "/bucket/path/blob", body).status());
        assertEquals(
            new TestHttpResponse(RestStatus.OK, body, addETag(S3HttpHandler.getEtagFromContents(body), TestHttpExchange.EMPTY_HEADERS)),
            handleRequest(handler, "GET", "/bucket/path/blob")
        );

        assertListObjectsResponse(handler, "", null, """
            <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix></Prefix><IsTruncated>false</IsTruncated>\
            <Contents><Key>path/blob</Key><Size>50</Size></Contents>\
            </ListBucketResult>""");

        assertListObjectsResponse(handler, "path/", null, """
            <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix>path/</Prefix><IsTruncated>false</IsTruncated>\
            <Contents><Key>path/blob</Key><Size>50</Size></Contents>\
            </ListBucketResult>""");

        assertListObjectsResponse(handler, "path/other", null, """
            <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix>path/other</Prefix><IsTruncated>false</IsTruncated>\
            </ListBucketResult>""");

        assertEquals(RestStatus.OK, handleRequest(handler, "PUT", "/bucket/path/subpath1/blob", randomAlphaOfLength(50)).status());
        assertEquals(RestStatus.OK, handleRequest(handler, "PUT", "/bucket/path/subpath2/blob", randomAlphaOfLength(50)).status());
        assertListObjectsResponse(handler, "path/", "/", """
            <?xml version="1.0" encoding="UTF-8"?><ListBucketResult>\
            <Prefix>path/</Prefix><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated>\
            <Contents><Key>path/blob</Key><Size>50</Size></Contents>\
            <CommonPrefixes><Prefix>path/subpath1/</Prefix></CommonPrefixes>\
            <CommonPrefixes><Prefix>path/subpath2/</Prefix></CommonPrefixes>\
            </ListBucketResult>""");

        assertEquals(RestStatus.OK, handleRequest(handler, "DELETE", "/bucket/path/blob").status());
        assertEquals(RestStatus.NO_CONTENT, handleRequest(handler, "DELETE", "/bucket/path/blob").status());

        assertListObjectsResponse(handler, "", null, """
            <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix></Prefix><IsTruncated>false</IsTruncated>\
            <Contents><Key>path/subpath1/blob</Key><Size>50</Size></Contents>\
            <Contents><Key>path/subpath2/blob</Key><Size>50</Size></Contents>\
            </ListBucketResult>""");

        assertEquals(RestStatus.OK, handleRequest(handler, "DELETE", "/bucket/path/subpath1/blob").status());
        assertEquals(RestStatus.OK, handleRequest(handler, "DELETE", "/bucket/path/subpath2/blob").status());

        assertListObjectsResponse(handler, "", null, """
            <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix></Prefix><IsTruncated>false</IsTruncated>\
            </ListBucketResult>""");
    }

    public void testGetWithBytesRange() {
        final var handler = new S3HttpHandler("bucket", "path", S3ConsistencyModel.randomConsistencyModel());
        final var blobName = "blob_name_" + randomIdentifier();
        final var blobPath = "/bucket/path/" + blobName;
        final var blobBytes = randomBytesReference(256);
        assertEquals(RestStatus.OK, handleRequest(handler, "PUT", blobPath, blobBytes).status());

        final var expectedEtag = S3HttpHandler.getEtagFromContents(blobBytes);

        assertEquals(
            "No Range",
            new TestHttpResponse(RestStatus.OK, blobBytes, addETag(expectedEtag, TestHttpExchange.EMPTY_HEADERS)),
            handleRequest(handler, "GET", blobPath)
        );

        var end = blobBytes.length() - 1;
        assertEquals(
            "Exact Range: bytes=0-" + end,
            new TestHttpResponse(
                RestStatus.PARTIAL_CONTENT,
                blobBytes,
                addETag(expectedEtag, contentRangeHeader(0, end, blobBytes.length()))
            ),
            handleRequest(handler, "GET", blobPath, BytesArray.EMPTY, bytesRangeHeader(0, end))
        );

        end = randomIntBetween(blobBytes.length() - 1, Integer.MAX_VALUE);
        assertEquals(
            "Larger Range: bytes=0-" + end,
            new TestHttpResponse(
                RestStatus.PARTIAL_CONTENT,
                blobBytes,
                addETag(expectedEtag, contentRangeHeader(0, blobBytes.length() - 1, blobBytes.length()))
            ),
            handleRequest(handler, "GET", blobPath, BytesArray.EMPTY, bytesRangeHeader(0, end))
        );

        var start = randomIntBetween(blobBytes.length(), Integer.MAX_VALUE - 1);
        end = randomIntBetween(start, Integer.MAX_VALUE);
        assertEquals(
            "Invalid Range: bytes=" + start + '-' + end,
            new TestHttpResponse(
                RestStatus.REQUESTED_RANGE_NOT_SATISFIED,
                BytesArray.EMPTY,
                addETag(expectedEtag, TestHttpExchange.EMPTY_HEADERS)
            ),
            handleRequest(handler, "GET", blobPath, BytesArray.EMPTY, bytesRangeHeader(start, end))
        );

        start = randomIntBetween(2, Integer.MAX_VALUE - 1);
        end = randomIntBetween(0, start - 1);
        assertEquals(
            "Weird Valid Range: bytes=" + start + '-' + end,
            new TestHttpResponse(RestStatus.OK, blobBytes, addETag(expectedEtag, TestHttpExchange.EMPTY_HEADERS)),
            handleRequest(handler, "GET", blobPath, BytesArray.EMPTY, bytesRangeHeader(start, end))
        );

        start = randomIntBetween(0, blobBytes.length() - 1);
        var length = randomIntBetween(1, blobBytes.length() - start);
        end = start + length - 1;
        assertEquals(
            "Range: bytes=" + start + '-' + end,
            new TestHttpResponse(
                RestStatus.PARTIAL_CONTENT,
                blobBytes.slice(start, length),
                addETag(expectedEtag, contentRangeHeader(start, end, blobBytes.length()))
            ),
            handleRequest(handler, "GET", blobPath, BytesArray.EMPTY, bytesRangeHeader(start, end))
        );
    }

    public void testSingleMultipartUpload() {
        final var handler = new S3HttpHandler("bucket", "path", S3ConsistencyModel.randomConsistencyModel());

        final var createUploadResponse = handleRequest(handler, "POST", "/bucket/path/blob?uploads");
        final var uploadId = getUploadId(createUploadResponse.body());
        assertEquals(new TestHttpResponse(RestStatus.OK, Strings.format("""
            <?xml version='1.0' encoding='UTF-8'?>\
            <InitiateMultipartUploadResult>\
            <Bucket>bucket</Bucket>\
            <Key>path/blob</Key>\
            <UploadId>%s</UploadId>\
            </InitiateMultipartUploadResult>""", uploadId)), createUploadResponse);

        final var part1 = randomAlphaOfLength(50);
        final var uploadPart1Response = handleRequest(handler, "PUT", "/bucket/path/blob?uploadId=" + uploadId + "&partNumber=1", part1);
        final var part1Etag = Objects.requireNonNull(uploadPart1Response.etag());
        assertEquals(new TestHttpResponse(RestStatus.OK, etagHeader(part1Etag)), uploadPart1Response);

        final var part2 = randomAlphaOfLength(50);
        final var uploadPart2Response = handleRequest(handler, "PUT", "/bucket/path/blob?uploadId=" + uploadId + "&partNumber=2", part2);
        final var part2Etag = Objects.requireNonNull(uploadPart2Response.etag());
        assertEquals(new TestHttpResponse(RestStatus.OK, etagHeader(part2Etag)), uploadPart2Response);

        assertEquals(
            new TestHttpResponse(RestStatus.OK, Strings.format("""
                <?xml version='1.0' encoding='UTF-8'?>\
                <ListMultipartUploadsResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>\
                <Bucket>bucket</Bucket><KeyMarker /><UploadIdMarker /><NextKeyMarker>--unused--</NextKeyMarker><NextUploadIdMarker />\
                <Delimiter /><Prefix>path/blob</Prefix><MaxUploads>10000</MaxUploads><IsTruncated>false</IsTruncated>\
                <Upload><Initiated>%s</Initiated><Key>path/blob</Key><UploadId>%s</UploadId></Upload>\
                </ListMultipartUploadsResult>""", handler.getUpload(uploadId).getInitiatedDateTime(), uploadId)),
            handleRequest(handler, "GET", "/bucket/?uploads&prefix=path/blob")
        );

        assertEquals(
            new TestHttpResponse(RestStatus.OK, """
                <?xml version="1.0" encoding="UTF-8"?>
                <CompleteMultipartUploadResult>
                <Bucket>bucket</Bucket>
                <Key>/bucket/path/blob</Key>
                </CompleteMultipartUploadResult>"""),
            handleRequest(handler, "POST", "/bucket/path/blob?uploadId=" + uploadId, Strings.format("""
                <?xml version="1.0" encoding="UTF-8"?>
                <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                   <Part>
                      <ETag>%s</ETag>
                      <PartNumber>1</PartNumber>
                   </Part>
                   <Part>
                      <ETag>%s</ETag>
                      <PartNumber>2</PartNumber>
                   </Part>
                </CompleteMultipartUpload>""", part1Etag, part2Etag))
        );

        assertListObjectsResponse(handler, "", null, """
            <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix></Prefix><IsTruncated>false</IsTruncated>\
            <Contents><Key>path/blob</Key><Size>100</Size></Contents>\
            </ListBucketResult>""");

        final var expectedContents = new BytesArray((part1 + part2).getBytes(StandardCharsets.UTF_8));
        assertEquals(
            new TestHttpResponse(
                RestStatus.OK,
                expectedContents,
                addETag(S3HttpHandler.getEtagFromContents(expectedContents), TestHttpExchange.EMPTY_HEADERS)
            ),
            handleRequest(handler, "GET", "/bucket/path/blob")
        );

        assertEquals(new TestHttpResponse(RestStatus.OK, """
            <?xml version='1.0' encoding='UTF-8'?>\
            <ListMultipartUploadsResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>\
            <Bucket>bucket</Bucket><KeyMarker /><UploadIdMarker /><NextKeyMarker>--unused--</NextKeyMarker><NextUploadIdMarker />\
            <Delimiter /><Prefix>path/blob</Prefix><MaxUploads>10000</MaxUploads><IsTruncated>false</IsTruncated>\
            </ListMultipartUploadsResult>"""), handleRequest(handler, "GET", "/bucket/?uploads&prefix=path/blob"));
    }

    public void testListAndAbortMultipartUpload() {
        final var handler = new S3HttpHandler("bucket", "path", S3ConsistencyModel.randomConsistencyModel());

        assertEquals(new TestHttpResponse(RestStatus.OK, """
            <?xml version='1.0' encoding='UTF-8'?>\
            <ListMultipartUploadsResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>\
            <Bucket>bucket</Bucket><KeyMarker /><UploadIdMarker /><NextKeyMarker>--unused--</NextKeyMarker><NextUploadIdMarker />\
            <Delimiter /><Prefix>path/blob</Prefix><MaxUploads>10000</MaxUploads><IsTruncated>false</IsTruncated>\
            </ListMultipartUploadsResult>"""), handleRequest(handler, "GET", "/bucket/?uploads&prefix=path/blob"));

        final var createUploadResponse = handleRequest(handler, "POST", "/bucket/path/blob?uploads");
        final var uploadId = getUploadId(createUploadResponse.body());
        assertEquals(new TestHttpResponse(RestStatus.OK, Strings.format("""
            <?xml version='1.0' encoding='UTF-8'?>\
            <InitiateMultipartUploadResult>\
            <Bucket>bucket</Bucket>\
            <Key>path/blob</Key>\
            <UploadId>%s</UploadId>\
            </InitiateMultipartUploadResult>""", uploadId)), createUploadResponse);

        final var part1 = randomAlphaOfLength(50);
        final var uploadPart1Response = handleRequest(handler, "PUT", "/bucket/path/blob?uploadId=" + uploadId + "&partNumber=1", part1);
        final var part1Etag = Objects.requireNonNull(uploadPart1Response.etag());
        assertEquals(new TestHttpResponse(RestStatus.OK, etagHeader(part1Etag)), uploadPart1Response);

        final var part2 = randomAlphaOfLength(50);
        final var uploadPart2Response = handleRequest(handler, "PUT", "/bucket/path/blob?uploadId=" + uploadId + "&partNumber=2", part2);
        final var part2Etag = Objects.requireNonNull(uploadPart2Response.etag());
        assertEquals(new TestHttpResponse(RestStatus.OK, etagHeader(part2Etag)), uploadPart2Response);

        assertEquals(
            new TestHttpResponse(RestStatus.OK, Strings.format("""
                <?xml version='1.0' encoding='UTF-8'?>\
                <ListMultipartUploadsResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>\
                <Bucket>bucket</Bucket><KeyMarker /><UploadIdMarker /><NextKeyMarker>--unused--</NextKeyMarker><NextUploadIdMarker />\
                <Delimiter /><Prefix>path/blob</Prefix><MaxUploads>10000</MaxUploads><IsTruncated>false</IsTruncated>\
                <Upload><Initiated>%s</Initiated><Key>path/blob</Key><UploadId>%s</UploadId></Upload>\
                </ListMultipartUploadsResult>""", handler.getUpload(uploadId).getInitiatedDateTime(), uploadId)),
            handleRequest(handler, "GET", "/bucket/?uploads&prefix=path/blob")
        );

        assertEquals(RestStatus.NO_CONTENT, handleRequest(handler, "DELETE", "/bucket/path/blob?uploadId=" + uploadId).status());

        assertEquals(new TestHttpResponse(RestStatus.OK, """
            <?xml version='1.0' encoding='UTF-8'?>\
            <ListMultipartUploadsResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>\
            <Bucket>bucket</Bucket><KeyMarker /><UploadIdMarker /><NextKeyMarker>--unused--</NextKeyMarker><NextUploadIdMarker />\
            <Delimiter /><Prefix>path/blob</Prefix><MaxUploads>10000</MaxUploads><IsTruncated>false</IsTruncated>\
            </ListMultipartUploadsResult>"""), handleRequest(handler, "GET", "/bucket/?uploads&prefix=path/blob"));

        final var completeUploadResponse = handleRequest(handler, "POST", "/bucket/path/blob?uploadId=" + uploadId, Strings.format("""
            <?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
               <Part>
                  <ETag>%s</ETag>
                  <PartNumber>1</PartNumber>
               </Part>
               <Part>
                  <ETag>%s</ETag>
                  <PartNumber>2</PartNumber>
               </Part>
            </CompleteMultipartUpload>""", part1Etag, part2Etag));
        if (completeUploadResponse.status() == RestStatus.OK) {
            // possible, but rare, indicating that S3 started processing the upload before returning an error
            assertThat(completeUploadResponse.body().utf8ToString(), allOf(containsString("<Error>"), containsString("NoSuchUpload")));
        } else {
            assertEquals(RestStatus.NOT_FOUND, completeUploadResponse.status());
        }
    }

    private static String getUploadId(BytesReference createUploadResponseBody) {
        return getUploadId(createUploadResponseBody.utf8ToString());
    }

    private static String getUploadId(String createUploadResponseBody) {
        final var startTag = "<UploadId>";
        final var startTagPosition = createUploadResponseBody.indexOf(startTag);
        assertThat(startTagPosition, greaterThan(0));
        final var startIdPosition = startTagPosition + startTag.length();
        return createUploadResponseBody.substring(startIdPosition, startIdPosition + 22);
    }

    public void testExtractPartEtags() {
        runExtractPartETagsTest("""
            <?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>""");

        runExtractPartETagsTest("""
            <?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
               <Part>
                  <ETag>etag1</ETag>
                  <PartNumber>1</PartNumber>
               </Part>
            </CompleteMultipartUpload>""", "etag1");

        runExtractPartETagsTest("""
            <?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
               <Part>
                  <ETag>etag1</ETag>
                  <PartNumber>1</PartNumber>
               </Part>
               <Part>
                  <ETag>etag2</ETag>
                  <PartNumber>2</PartNumber>
               </Part>
            </CompleteMultipartUpload>""", "etag1", "etag2");

        runExtractPartETagsTest("""
            <?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
               <Part>
                  <ETag>etag2</ETag>
                  <PartNumber>2</PartNumber>
               </Part>
               <Part>
                  <ETag>etag1</ETag>
                  <PartNumber>1</PartNumber>
               </Part>
            </CompleteMultipartUpload>""", "etag1", "etag2");

        expectThrows(IllegalStateException.class, () -> runExtractPartETagsTest("""
            <?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
               <Part>
                  <ETag>etag2</ETag>
                  <PartNumber>2</PartNumber>
               </Part>
            </CompleteMultipartUpload>"""));

    }

    public void testPreventObjectOverwrite() {
        ensureExactlyOneSuccess(new S3HttpHandler("bucket", "path", S3ConsistencyModel.AWS_DEFAULT), null);
    }

    public void testConditionalOverwrite() {
        final var handler = new S3HttpHandler("bucket", "path", S3ConsistencyModel.AWS_DEFAULT);

        final var originalBody = new BytesArray(randomAlphaOfLength(50).getBytes(StandardCharsets.UTF_8));
        final var originalETag = S3HttpHandler.getEtagFromContents(originalBody);
        assertEquals(RestStatus.OK, handleRequest(handler, "PUT", "/bucket/path/blob", originalBody).status());
        assertEquals(
            new TestHttpResponse(RestStatus.OK, originalBody, addETag(originalETag, TestHttpExchange.EMPTY_HEADERS)),
            handleRequest(handler, "GET", "/bucket/path/blob")
        );

        ensureExactlyOneSuccess(handler, originalETag);
    }

    private static void ensureExactlyOneSuccess(S3HttpHandler handler, String originalETag) {
        final var tasks = List.of(
            createPutObjectTask(handler, originalETag),
            createPutObjectTask(handler, originalETag),
            createMultipartUploadTask(handler, originalETag),
            createMultipartUploadTask(handler, originalETag)
        );

        runInParallel(tasks.size(), i -> tasks.get(i).consumer.run());

        List<TestWriteTask> successfulTasks = tasks.stream().filter(task -> task.status == RestStatus.OK).toList();
        assertThat(successfulTasks, hasSize(1));

        tasks.stream().filter(task -> task.uploadId != null).forEach(task -> {
            if (task.status == RestStatus.PRECONDITION_FAILED) {
                assertNotNull(handler.getUpload(task.uploadId));
            } else {
                assertThat(task.status, oneOf(RestStatus.OK, RestStatus.CONFLICT));
                assertNull(handler.getUpload(task.uploadId));
            }
        });

        assertEquals(
            new TestHttpResponse(
                RestStatus.OK,
                successfulTasks.getFirst().body,
                addETag(S3HttpHandler.getEtagFromContents(successfulTasks.getFirst().body), TestHttpExchange.EMPTY_HEADERS)
            ),
            handleRequest(handler, "GET", "/bucket/path/blob")
        );
    }

    public void testPutObjectIfMatchWithBlobNotFound() {
        final var handler = new S3HttpHandler("bucket", "path", S3ConsistencyModel.AWS_DEFAULT);
        while (true) {
            final var task = createPutObjectTask(handler, randomIdentifier());
            task.consumer.run();
            if (task.status == RestStatus.NOT_FOUND) {
                break;
            }
            assertEquals(RestStatus.CONFLICT, task.status); // chosen randomly so eventually we will escape the loop
        }
    }

    public void testCompleteMultipartUploadIfMatchWithBlobNotFound() {
        final var handler = new S3HttpHandler("bucket", "path", S3ConsistencyModel.AWS_DEFAULT);
        while (true) {
            final var task = createMultipartUploadTask(handler, randomIdentifier());
            task.consumer.run();
            if (task.status == RestStatus.NOT_FOUND) {
                break;
            }
            assertEquals(RestStatus.CONFLICT, task.status); // chosen randomly so eventually we will escape the loop
        }
    }

    private static TestWriteTask createPutObjectTask(S3HttpHandler handler, @Nullable String originalETag) {
        return new TestWriteTask(
            (task) -> task.status = handleRequest(handler, "PUT", "/bucket/path/blob", task.body, conditionalWriteHeader(originalETag))
                .status()
        );
    }

    private static TestWriteTask createMultipartUploadTask(S3HttpHandler handler, @Nullable String originalETag) {
        final var multipartUploadTask = new TestWriteTask(
            (task) -> task.status = handleRequest(
                handler,
                "POST",
                "/bucket/path/blob?uploadId=" + task.uploadId,
                new BytesArray(Strings.format("""
                    <?xml version="1.0" encoding="UTF-8"?>
                    <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                       <Part>
                          <ETag>%s</ETag>
                          <PartNumber>1</PartNumber>
                       </Part>
                    </CompleteMultipartUpload>""", task.etag)),
                conditionalWriteHeader(originalETag)
            ).status()
        );

        final var createUploadResponse = handleRequest(handler, "POST", "/bucket/path/blob?uploads");
        multipartUploadTask.uploadId = getUploadId(createUploadResponse.body());

        final var uploadPart1Response = handleRequest(
            handler,
            "PUT",
            "/bucket/path/blob?uploadId=" + multipartUploadTask.uploadId + "&partNumber=1",
            multipartUploadTask.body
        );
        multipartUploadTask.etag = Objects.requireNonNull(uploadPart1Response.etag());

        return multipartUploadTask;
    }

    public void testGetETagFromContents() {
        // empty-string value from Wikipedia, see also org.elasticsearch.common.hash.MessageDigestsTests.testSha256
        assertETag("", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertETag("The quick brown fox jumps over the lazy dog", "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592");
        assertETag("The quick brown fox jumps over the lazy cog", "e4c4d8f3bf76b692de791a173e05321150f7a345b46484fe427f6acc7ecc81be");
    }

    private static void assertETag(String input, String expectedHash) {
        assertEquals(
            "\"es-test-sha-256-" + expectedHash + '"',
            S3HttpHandler.getEtagFromContents(new BytesArray(input.getBytes(StandardCharsets.UTF_8)))
        );
    }

    private static class TestWriteTask {
        final BytesReference body;
        final Runnable consumer;
        String uploadId;
        String etag;
        RestStatus status;

        TestWriteTask(Consumer<TestWriteTask> consumer) {
            this.body = randomBytesReference(50);
            this.consumer = () -> consumer.accept(this);
        }
    }

    private void runExtractPartETagsTest(String body, String... expectedTags) {
        assertEquals(List.of(expectedTags), S3HttpHandler.extractPartEtags(new BytesArray(body.getBytes(StandardCharsets.UTF_8))));
    }

    private record TestHttpResponse(RestStatus status, BytesReference body, Headers headers) {
        TestHttpResponse(RestStatus status, String body) {
            this(status, new BytesArray(body.getBytes(StandardCharsets.UTF_8)), TestHttpExchange.EMPTY_HEADERS);
        }

        TestHttpResponse(RestStatus status, Headers headers) {
            this(status, BytesArray.EMPTY, headers);
        }

        String etag() {
            return headers.getFirst("ETag");
        }
    }

    private static TestHttpResponse handleRequest(S3HttpHandler handler, String method, String uri) {
        return handleRequest(handler, method, uri, "");
    }

    private static TestHttpResponse handleRequest(S3HttpHandler handler, String method, String uri, String requestBody) {
        return handleRequest(handler, method, uri, new BytesArray(requestBody.getBytes(StandardCharsets.UTF_8)));
    }

    private static TestHttpResponse handleRequest(S3HttpHandler handler, String method, String uri, BytesReference requestBody) {
        return handleRequest(handler, method, uri, requestBody, TestHttpExchange.EMPTY_HEADERS);
    }

    private static TestHttpResponse handleRequest(
        S3HttpHandler handler,
        String method,
        String uri,
        BytesReference requestBody,
        Headers requestHeaders
    ) {
        final var httpExchange = new TestHttpExchange(method, uri, requestBody, requestHeaders);
        try {
            handler.handle(httpExchange);
        } catch (IOException e) {
            fail(e);
        }
        assertNotEquals(0, httpExchange.getResponseCode());
        var responseHeaders = new Headers();
        httpExchange.getResponseHeaders().forEach((header, values) -> {
            // com.sun.net.httpserver.Headers.Headers() normalize keys
            if ("Etag".equals(header) || "Content-range".equals(header)) {
                responseHeaders.put(header, List.copyOf(values));
            }
        });
        return new TestHttpResponse(
            RestStatus.fromCode(httpExchange.getResponseCode()),
            httpExchange.getResponseBodyContents(),
            responseHeaders
        );
    }

    private static Headers bytesRangeHeader(@Nullable Integer startInclusive, @Nullable Integer endInclusive) {
        StringBuilder range = new StringBuilder("bytes=");
        if (startInclusive != null) {
            range.append(startInclusive);
        }
        range.append('-');
        if (endInclusive != null) {
            range.append(endInclusive);
        }
        var headers = new Headers();
        headers.put("Range", List.of(range.toString()));
        return headers;
    }

    private static Headers etagHeader(String etag) {
        var headers = new Headers();
        headers.put("ETag", List.of(Objects.requireNonNull(etag)));
        return headers;
    }

    private static Headers contentRangeHeader(long start, long end, long length) {
        var headers = new Headers();
        headers.put("Content-Range", List.of(Strings.format("bytes %d-%d/%d", start, end, length)));
        return headers;
    }

    private static Headers conditionalWriteHeader(@Nullable String originalEtag) {
        var headers = new Headers();
        if (originalEtag == null) {
            headers.put("If-None-Match", List.of("*"));
        } else {
            headers.put("If-Match", List.of(originalEtag));
        }
        return headers;
    }

    private static Headers addETag(String eTag, Headers headers) {
        final var newHeaders = new Headers(headers);
        newHeaders.add("ETag", eTag);
        return newHeaders;
    }

    private static class TestHttpExchange extends HttpExchange {

        private static final Headers EMPTY_HEADERS = new Headers();

        private final String method;
        private final URI uri;
        private final BytesReference requestBody;
        private final Headers requestHeaders;

        private final Headers responseHeaders = new Headers();
        private final BytesStreamOutput responseBody = new BytesStreamOutput();
        private int responseCode;

        TestHttpExchange(String method, String uri, BytesReference requestBody, Headers requestHeaders) {
            this.method = method;
            this.uri = URI.create(uri);
            this.requestBody = requestBody;
            this.requestHeaders = requestHeaders;
        }

        @Override
        public Headers getRequestHeaders() {
            return requestHeaders;
        }

        @Override
        public Headers getResponseHeaders() {
            return responseHeaders;
        }

        @Override
        public URI getRequestURI() {
            return uri;
        }

        @Override
        public String getRequestMethod() {
            return method;
        }

        @Override
        public HttpContext getHttpContext() {
            return null;
        }

        @Override
        public void close() {}

        @Override
        public InputStream getRequestBody() {
            try {
                return requestBody.streamInput();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public OutputStream getResponseBody() {
            return responseBody;
        }

        @Override
        public void sendResponseHeaders(int rCode, long responseLength) {
            this.responseCode = rCode;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public int getResponseCode() {
            return responseCode;
        }

        public BytesReference getResponseBodyContents() {
            return responseBody.bytes();
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public String getProtocol() {
            return "HTTP/1.1";
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public void setAttribute(String name, Object value) {
            fail("setAttribute not implemented");
        }

        @Override
        public void setStreams(InputStream i, OutputStream o) {
            fail("setStreams not implemented");
        }

        @Override
        public HttpPrincipal getPrincipal() {
            fail("getPrincipal not implemented");
            throw new UnsupportedOperationException("getPrincipal not implemented");
        }
    }

}
