/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.gcs;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.fixture.HttpHeaderParser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static java.util.Objects.requireNonNull;

public class GoogleCloudStorageHttpHandlerTests extends ESTestCase {

    private static final String HOST = "http://127.0.0.1:12345";
    private static final int RESUME_INCOMPLETE = 308;
    private static final Pattern GENERATION_PATTERN = Pattern.compile("\"generation\"\\s*:\\s*\"(\\d+)\"");

    public void testRejectsBadUri() {
        assertEquals(
            RestStatus.NOT_FOUND.getStatus(),
            handleRequest(new GoogleCloudStorageHttpHandler("bucket"), randomFrom("GET", "PUT", "POST", "DELETE", "HEAD"), "/not-in-bucket")
                .status()
        );
    }

    public void testCheckEndpoint() {
        final var handler = new GoogleCloudStorageHttpHandler("bucket");

        assertEquals(
            RestStatus.OK,
            handleRequest(handler, "GET", "/", BytesArray.EMPTY, Headers.of("Metadata-Flavor", "Google")).restStatus()
        );
    }

    public void testSimpleObjectOperations() {
        final var bucket = randomAlphaOfLength(10);
        final var handler = new GoogleCloudStorageHttpHandler(bucket);
        final var blobName = "path/" + randomAlphaOfLength(10);

        assertEquals(RestStatus.NOT_FOUND, getBlobContents(handler, bucket, blobName, null, null).restStatus());

        assertEquals(
            new TestHttpResponse(RestStatus.OK, "{\"kind\":\"storage#objects\",\"items\":[],\"prefixes\":[]}"),
            listBlobs(handler, bucket, null, null)
        );

        final var body = randomAlphaOfLength(50);
        assertEquals(
            RestStatus.OK,
            executeUpload(handler, bucket, blobName, new BytesArray(body.getBytes(StandardCharsets.UTF_8)), null).restStatus()
        );

        assertEquals(new TestHttpResponse(RestStatus.OK, body), getBlobContents(handler, bucket, blobName, null, null));

        assertEquals(new TestHttpResponse(RestStatus.OK, Strings.format("""
            {"kind":"storage#objects","items":[{"kind":"storage#object","bucket":"%s","name":"%s","id":"%s","size":"50",\
            "generation":"1"}],"prefixes":[]}""", bucket, blobName, blobName)), listBlobs(handler, bucket, null, null));

        assertEquals(new TestHttpResponse(RestStatus.OK, Strings.format("""
            {"kind":"storage#objects","items":[{"kind":"storage#object","bucket":"%s","name":"%s","id":"%s","size":"50",\
            "generation":"1"}],"prefixes":[]}""", bucket, blobName, blobName)), listBlobs(handler, bucket, "path/", null));

        assertEquals(new TestHttpResponse(RestStatus.OK, """
            {"kind":"storage#objects","items":[],"prefixes":[]}"""), listBlobs(handler, bucket, "some/other/path", null));

        var boundary = newMultipartBoundary();
        assertEquals(
            new TestHttpResponse(RestStatus.OK, """
                --$boundary
                Content-Length: 168
                Content-Type: application/http
                content-id: 1
                content-transfer-encoding: binary

                HTTP/1.1 204 NO_CONTENT




                --$boundary--
                """.replace("\n", "\r\n").replace("$boundary", boundary)),
            handleRequest(
                handler,
                "POST",
                "/batch/storage/v1",
                createBatchDeleteRequest(bucket, boundary, blobName),
                Headers.of("Content-Type", "mixed/multipart")
            )
        );
        assertEquals(
            RestStatus.OK,
            handleRequest(
                handler,
                "POST",
                "/batch/storage/v1",
                createBatchDeleteRequest(bucket, boundary, blobName),
                Headers.of("Content-Type", "mixed/multipart")
            ).restStatus()
        );

        assertEquals(new TestHttpResponse(RestStatus.OK, """
            {"kind":"storage#objects","items":[],"prefixes":[]}"""), listBlobs(handler, bucket, "path/", null));
    }

    public void testGetWithBytesRange() {
        final var bucket = randomIdentifier();
        final var handler = new GoogleCloudStorageHttpHandler(bucket);
        final var blobName = "blob_name_" + randomIdentifier();
        final var blobBytes = randomBytesReference(256);

        assertEquals(RestStatus.OK, executeUpload(handler, bucket, blobName, blobBytes, 0L).restStatus());

        assertEquals(
            "No Range",
            new TestHttpResponse(RestStatus.OK, blobBytes, TestHttpExchange.EMPTY_HEADERS),
            getBlobContents(handler, bucket, blobName, null, null)
        );

        var end = blobBytes.length() - 1;
        assertEquals(
            "Exact Range: bytes=0-" + end,
            new TestHttpResponse(RestStatus.PARTIAL_CONTENT, blobBytes, TestHttpExchange.EMPTY_HEADERS),
            getBlobContents(handler, bucket, blobName, null, new HttpHeaderParser.Range(0, end))
        );

        end = randomIntBetween(blobBytes.length() - 1, Integer.MAX_VALUE);
        assertEquals(
            "Larger Range: bytes=0-" + end,
            new TestHttpResponse(RestStatus.PARTIAL_CONTENT, blobBytes, TestHttpExchange.EMPTY_HEADERS),
            getBlobContents(handler, bucket, blobName, null, new HttpHeaderParser.Range(0, end))
        );

        var start = randomIntBetween(blobBytes.length(), Integer.MAX_VALUE - 1);
        end = randomIntBetween(start, Integer.MAX_VALUE);
        assertEquals(
            "Invalid Range: bytes=" + start + '-' + end,
            new TestHttpResponse(RestStatus.REQUESTED_RANGE_NOT_SATISFIED, BytesArray.EMPTY, TestHttpExchange.EMPTY_HEADERS),
            getBlobContents(handler, bucket, blobName, null, new HttpHeaderParser.Range(start, end))
        );

        start = randomIntBetween(0, blobBytes.length() - 1);
        var length = randomIntBetween(1, blobBytes.length() - start);
        end = start + length - 1;
        assertEquals(
            "Range: bytes=" + start + '-' + end,
            new TestHttpResponse(RestStatus.PARTIAL_CONTENT, blobBytes.slice(start, length), TestHttpExchange.EMPTY_HEADERS),
            getBlobContents(handler, bucket, blobName, null, new HttpHeaderParser.Range(start, end))
        );
    }

    public void testZeroLengthObjectGets() {
        final var bucket = randomIdentifier();
        final var handler = new GoogleCloudStorageHttpHandler(bucket);
        final var blobName = "blob_name_" + randomIdentifier();
        final var blobBytes = BytesArray.EMPTY;

        assertEquals(RestStatus.OK, executeMultipartUpload(handler, bucket, blobName, blobBytes, 0L).restStatus());

        assertEquals(
            "No Range",
            new TestHttpResponse(RestStatus.OK, blobBytes, TestHttpExchange.EMPTY_HEADERS),
            getBlobContents(handler, bucket, blobName, null, null)
        );

        assertEquals(
            "Range 0-0",
            new TestHttpResponse(RestStatus.REQUESTED_RANGE_NOT_SATISFIED, BytesArray.EMPTY, TestHttpExchange.EMPTY_HEADERS),
            getBlobContents(handler, bucket, blobName, null, new HttpHeaderParser.Range(0, 0))
        );

        assertEquals(
            "Random range x-y",
            new TestHttpResponse(RestStatus.REQUESTED_RANGE_NOT_SATISFIED, BytesArray.EMPTY, TestHttpExchange.EMPTY_HEADERS),
            getBlobContents(handler, bucket, blobName, null, new HttpHeaderParser.Range(randomIntBetween(0, 30), randomIntBetween(31, 100)))
        );
    }

    public void testResumableUpload() {
        final var bucket = randomIdentifier();
        final var handler = new GoogleCloudStorageHttpHandler(bucket);
        final var blobName = "blob_name_" + randomIdentifier();

        final var createUploadResponse = handleRequest(
            handler,
            "POST",
            "/upload/storage/v1/b/" + bucket + "/?uploadType=resumable&name=" + blobName
        );
        final var locationHeader = createUploadResponse.headers.getFirst("Location");
        final var sessionURI = locationHeader.substring(locationHeader.indexOf(HOST) + HOST.length());
        assertEquals(RestStatus.OK, createUploadResponse.restStatus());

        // status check
        assertEquals(
            new TestHttpResponse(RESUME_INCOMPLETE, TestHttpExchange.EMPTY_HEADERS),
            handleRequest(handler, "PUT", sessionURI, BytesArray.EMPTY, contentRangeHeader(null, null, null))
        );

        final var part1 = randomAlphaOfLength(50);
        final var uploadPart1Response = handleRequest(handler, "PUT", sessionURI, part1, contentRangeHeader(0, 50, null));
        assertEquals(new TestHttpResponse(RESUME_INCOMPLETE, rangeHeader(0, 49)), uploadPart1Response);

        // status check
        assertEquals(
            new TestHttpResponse(RESUME_INCOMPLETE, rangeHeader(0, 49)),
            handleRequest(handler, "PUT", sessionURI, BytesArray.EMPTY, contentRangeHeader(null, null, null))
        );

        final var part2 = randomAlphaOfLength(50);
        final var uploadPart2Response = handleRequest(handler, "PUT", sessionURI, part2, contentRangeHeader(50, 99, null));
        assertEquals(new TestHttpResponse(RESUME_INCOMPLETE, rangeHeader(0, 99)), uploadPart2Response);

        // incomplete upload should not be visible yet
        assertEquals(RestStatus.NOT_FOUND, getBlobContents(handler, bucket, blobName, null, null).restStatus());

        final var part3 = randomAlphaOfLength(30);
        final var uploadPart3Response = handleRequest(handler, "PUT", sessionURI, part3, contentRangeHeader(100, 129, 130));
        assertEquals(new TestHttpResponse(RestStatus.OK, rangeHeader(0, 129)), uploadPart3Response);

        // status check
        assertEquals(
            new TestHttpResponse(RestStatus.OK, rangeHeader(0, 129)),
            handleRequest(handler, "PUT", sessionURI, BytesArray.EMPTY, contentRangeHeader(null, null, null))
        );

        // complete upload should be visible now

        // can download contents
        assertEquals(
            new TestHttpResponse(RestStatus.OK, part1 + part2 + part3),
            handleRequest(handler, "GET", "/download/storage/v1/b/" + bucket + "/o/" + blobName)
        );

        // can see in listing
        assertEquals(
            new TestHttpResponse(RestStatus.OK, Strings.format("""
                {"kind":"storage#objects","items":[{"kind":"storage#object","bucket":"%s","name":"%s","id":"%s","size":"130",\
                "generation":"1"}],"prefixes":[]}""", bucket, blobName, blobName)),
            handleRequest(handler, "GET", "/storage/v1/b/" + bucket + "/o")
        );

        // can get metadata
        assertEquals(
            new TestHttpResponse(
                RestStatus.OK,
                Strings.format(
                    """
                        {"kind":"storage#object","bucket":"%s","name":"%s","id":"%s","size":"130","generation":"1"}""",
                    bucket,
                    blobName,
                    blobName
                )
            ),
            handleRequest(handler, "GET", "/storage/v1/b/" + bucket + "/o/" + blobName)
        );
    }

    public void testIfGenerationMatch_MultipartUpload() {
        final var bucket = randomIdentifier();
        final var handler = new GoogleCloudStorageHttpHandler(bucket);
        final var blobName = "blob_name_" + randomIdentifier();

        assertEquals(
            RestStatus.OK,
            executeUpload(handler, bucket, blobName, randomBytesReference(randomIntBetween(100, 5_000)), null).restStatus()
        );

        // update, matched generation
        assertEquals(
            RestStatus.OK,
            executeMultipartUpload(
                handler,
                bucket,
                blobName,
                randomBytesReference(randomIntBetween(100, 5_000)),
                getCurrentGeneration(handler, bucket, blobName)
            ).restStatus()
        );

        // update, mismatched generation
        assertEquals(
            RestStatus.PRECONDITION_FAILED,
            executeMultipartUpload(
                handler,
                bucket,
                blobName,
                randomBytesReference(randomIntBetween(100, 5_000)),
                randomValueOtherThan(getCurrentGeneration(handler, bucket, blobName), ESTestCase::randomNonNegativeLong)
            ).restStatus()
        );

        // update, no generation
        assertEquals(
            RestStatus.OK,
            executeMultipartUpload(handler, bucket, blobName, randomBytesReference(randomIntBetween(100, 5_000)), null).restStatus()
        );

        // update, zero generation
        assertEquals(
            RestStatus.PRECONDITION_FAILED,
            executeMultipartUpload(handler, bucket, blobName, randomBytesReference(randomIntBetween(100, 5_000)), 0L).restStatus()
        );

        // new file, zero generation
        assertEquals(
            RestStatus.OK,
            executeMultipartUpload(handler, bucket, blobName + "/new/1", randomBytesReference(randomIntBetween(100, 5_000)), 0L)
                .restStatus()
        );

        // new file, non-zero generation
        assertEquals(
            RestStatus.PRECONDITION_FAILED,
            executeMultipartUpload(
                handler,
                bucket,
                blobName + "/new/2",
                randomBytesReference(randomIntBetween(100, 5_000)),
                randomLongBetween(1, Long.MAX_VALUE)
            ).restStatus()
        );
    }

    public void testIfGenerationMatch_ResumableUpload() {
        final var bucket = randomIdentifier();
        final var handler = new GoogleCloudStorageHttpHandler(bucket);
        final var blobName = "blob_name_" + randomIdentifier();

        assertEquals(
            RestStatus.OK,
            executeUpload(handler, bucket, blobName, randomBytesReference(randomIntBetween(100, 5_000)), null).restStatus()
        );

        // update, matched generation
        assertEquals(
            RestStatus.OK,
            executeResumableUpload(
                handler,
                bucket,
                blobName,
                randomBytesReference(randomIntBetween(100, 5_000)),
                getCurrentGeneration(handler, bucket, blobName)
            ).restStatus()
        );

        // update, mismatched generation
        assertEquals(
            RestStatus.PRECONDITION_FAILED,
            executeResumableUpload(
                handler,
                bucket,
                blobName,
                randomBytesReference(randomIntBetween(100, 5_000)),
                randomValueOtherThan(getCurrentGeneration(handler, bucket, blobName), ESTestCase::randomNonNegativeLong)
            ).restStatus()
        );

        // update, no generation
        assertEquals(
            RestStatus.OK,
            executeResumableUpload(handler, bucket, blobName, randomBytesReference(randomIntBetween(100, 5_000)), null).restStatus()
        );

        // update, zero generation
        assertEquals(
            RestStatus.PRECONDITION_FAILED,
            executeResumableUpload(handler, bucket, blobName, randomBytesReference(randomIntBetween(100, 5_000)), 0L).restStatus()
        );

        // new file, zero generation
        assertEquals(
            RestStatus.OK,
            executeResumableUpload(handler, bucket, blobName + "/new/1", randomBytesReference(randomIntBetween(100, 5_000)), 0L)
                .restStatus()
        );

        // new file, non-zero generation
        assertEquals(
            RestStatus.PRECONDITION_FAILED,
            executeResumableUpload(
                handler,
                bucket,
                blobName + "/new/2",
                randomBytesReference(randomIntBetween(100, 5_000)),
                randomLongBetween(1, Long.MAX_VALUE)
            ).restStatus()
        );
    }

    public void testIfGenerationMatch_GetObject() {
        final var bucket = randomIdentifier();
        final var handler = new GoogleCloudStorageHttpHandler(bucket);
        final var blobName = "blob_name_" + randomIdentifier();

        assertEquals(
            RestStatus.OK,
            executeUpload(handler, bucket, blobName, randomBytesReference(randomIntBetween(100, 5_000)), null).restStatus()
        );

        final long currentGeneration = getCurrentGeneration(handler, bucket, blobName);

        // Get contents, matching generation
        assertEquals(RestStatus.OK, getBlobContents(handler, bucket, blobName, currentGeneration, null).restStatus());

        // Get contents, mismatched generation
        assertEquals(
            RestStatus.PRECONDITION_FAILED,
            getBlobContents(handler, bucket, blobName, randomValueOtherThan(currentGeneration, ESTestCase::randomNonNegativeLong), null)
                .restStatus()
        );

        // Get metadata, matching generation
        assertEquals(RestStatus.OK, getBlobMetadata(handler, bucket, blobName, currentGeneration).restStatus());

        // Get metadata, mismatched generation
        assertEquals(
            RestStatus.PRECONDITION_FAILED,
            getBlobMetadata(handler, bucket, blobName, randomValueOtherThan(currentGeneration, ESTestCase::randomNonNegativeLong))
                .restStatus()
        );
    }

    public void testListObjectsWithPrefix() {
        final var bucket = randomIdentifier();
        final var handler = new GoogleCloudStorageHttpHandler(bucket);

        final int numberOfFiles = randomIntBetween(1, 100);
        final int numberWithMatchingPrefix = randomIntBetween(0, numberOfFiles);
        final String prefix = randomIdentifier();

        // Create expected state
        for (int i = 0; i < numberOfFiles; i++) {
            final String blobName;
            if (i < numberWithMatchingPrefix) {
                blobName = prefix + "blob_name_" + i;
            } else {
                final String nonMatchingPrefix = randomValueOtherThan(prefix, ESTestCase::randomIdentifier);
                blobName = nonMatchingPrefix + "blob_name_" + i;
            }
            assertEquals(
                RestStatus.OK,
                executeUpload(handler, bucket, blobName, randomBytesReference(randomIntBetween(100, 5_000)), null).restStatus()
            );
        }

        TestHttpResponse response = listBlobs(handler, bucket, prefix, null);
        assertEquals(RestStatus.OK, response.restStatus());

        XContentTestUtils.JsonMapView jsonMapView = XContentTestUtils.createJsonMapView(
            new ByteArrayInputStream(BytesReference.toBytes(response.body()))
        );
        assertEquals(numberWithMatchingPrefix, ((List<?>) jsonMapView.get("items")).size());
    }

    public void testListObjectsWithPrefixAndDelimiter() {
        final var bucket = randomIdentifier();
        final var handler = new GoogleCloudStorageHttpHandler(bucket);
        final var delimiter = randomFrom("/", ".", "+", "\\");
        final var prefix = randomBoolean() ? "" : randomIdentifier() + delimiter;

        final int numberOfFiles = randomIntBetween(1, 100);
        final int numberWithDelimiter = randomIntBetween(0, numberOfFiles);

        // Create expected state
        final Set<String> topLevelDirectories = new HashSet<>();
        for (int i = 0; i < numberOfFiles; i++) {
            final String blobName;
            if (i < numberWithDelimiter) {
                final String directory = randomAlphaOfLength(3);
                blobName = directory + delimiter + "blob_name_" + i;
                topLevelDirectories.add(directory + delimiter);
            } else {
                blobName = randomIdentifier() + "_blob_name_" + i;
            }
            assertEquals(
                RestStatus.OK,
                executeUpload(handler, bucket, prefix + blobName, randomBytesReference(randomIntBetween(100, 5_000)), null).restStatus()
            );
        }

        final TestHttpResponse response = listBlobs(handler, bucket, prefix, delimiter);
        assertEquals(RestStatus.OK, response.restStatus());

        XContentTestUtils.JsonMapView jsonMapView = XContentTestUtils.createJsonMapView(
            new ByteArrayInputStream(BytesReference.toBytes(response.body()))
        );
        assertEquals(numberOfFiles - numberWithDelimiter, ((List<?>) jsonMapView.get("items")).size());
        assertEquals(
            topLevelDirectories.stream().map(d -> prefix + d).collect(Collectors.toSet()),
            new HashSet<>(jsonMapView.get("prefixes"))
        );
    }

    /**
     * Tests the example from <a href="https://cloud.google.com/storage/docs/json_api/v1/objects/list">The docs</a>
     */
    public void testListObjectsExampleFromDocumentation() {
        final var bucket = randomIdentifier();
        final var handler = new GoogleCloudStorageHttpHandler(bucket);

        Stream.of("a/b", "a/c", "d", "e", "e/f", "e/g/h")
            .forEach(
                path -> assertEquals(
                    RestStatus.OK,
                    executeUpload(handler, bucket, path, randomBytesReference(randomIntBetween(100, 5_000)), null).restStatus()
                )
            );

        TestHttpResponse response = listBlobs(handler, bucket, null, "/");
        assertEquals(RestStatus.OK, response.restStatus());
        XContentTestUtils.JsonMapView jsonMapView = XContentTestUtils.createJsonMapView(
            new ByteArrayInputStream(BytesReference.toBytes(response.body()))
        );
        assertEquals(
            Set.of("d", "e"),
            ((List<?>) jsonMapView.get("items")).stream().map(i -> ((Map<?, ?>) i).get("name")).collect(Collectors.toSet())
        );
        assertEquals(Set.of("a/", "e/"), new HashSet<>(jsonMapView.get("prefixes")));

        response = listBlobs(handler, bucket, "e/", "/");
        assertEquals(RestStatus.OK, response.restStatus());
        jsonMapView = XContentTestUtils.createJsonMapView(new ByteArrayInputStream(BytesReference.toBytes(response.body())));
        assertEquals(
            Set.of("e/f"),
            ((List<?>) jsonMapView.get("items")).stream().map(i -> ((Map<?, ?>) i).get("name")).collect(Collectors.toSet())
        );
        // note this differs from the example, but third party test indicates this is what we get back
        assertEquals(Set.of("e/g/"), new HashSet<>(jsonMapView.get("prefixes")));
    }

    private static TestHttpResponse executeUpload(
        GoogleCloudStorageHttpHandler handler,
        String bucket,
        String blobName,
        BytesReference bytes,
        Long ifGenerationMatch
    ) {
        if (randomBoolean()) {
            return executeResumableUpload(handler, bucket, blobName, bytes, ifGenerationMatch);
        } else {
            return executeMultipartUpload(handler, bucket, blobName, bytes, ifGenerationMatch);
        }
    }

    private static TestHttpResponse executeResumableUpload(
        GoogleCloudStorageHttpHandler handler,
        String bucket,
        String blobName,
        BytesReference bytes,
        Long ifGenerationMatch
    ) {
        assert bytes.length() >= 2 : "We can't split anything smaller than two";
        final var createUploadResponse = handleRequest(
            handler,
            "POST",
            "/upload/storage/v1/b/"
                + bucket
                + "/"
                + generateQueryString("uploadType", "resumable", "name", blobName, "ifGenerationMatch", ifGenerationMatch)
        );
        final var locationHeader = createUploadResponse.headers.getFirst("Location");
        final var sessionURI = locationHeader.substring(locationHeader.indexOf(HOST) + HOST.length());
        assertEquals(RestStatus.OK, createUploadResponse.restStatus());

        final int partBoundary = randomIntBetween(1, bytes.length() - 1);
        final var part1 = bytes.slice(0, partBoundary);
        final var uploadPart1Response = handleRequest(handler, "PUT", sessionURI, part1, contentRangeHeader(0, partBoundary - 1, null));
        assertEquals(RESUME_INCOMPLETE, uploadPart1Response.status());

        final var part2 = bytes.slice(partBoundary, bytes.length() - partBoundary);
        return handleRequest(handler, "PUT", sessionURI, part2, contentRangeHeader(partBoundary, bytes.length() - 1, bytes.length()));
    }

    private static TestHttpResponse executeMultipartUpload(
        GoogleCloudStorageHttpHandler handler,
        String bucket,
        String blobName,
        BytesReference bytes,
        Long ifGenerationMatch
    ) {
        var headers = new Headers();
        // multipart upload is required to provide boundary header
        var boundary = newMultipartBoundary();
        headers.put("Content-Type", List.of("multipart/related; boundary=" + boundary));
        return handleRequest(
            handler,
            "POST",
            "/upload/storage/v1/b/" + bucket + "/" + generateQueryString("uploadType", "multipart", "ifGenerationMatch", ifGenerationMatch),
            createGzipCompressedMultipartUploadBody(bucket, blobName, bytes, boundary),
            headers
        );
    }

    private static TestHttpResponse getBlobContents(
        GoogleCloudStorageHttpHandler handler,
        String bucket,
        String blobName,
        @Nullable Long ifGenerationMatch,
        @Nullable HttpHeaderParser.Range range
    ) {
        return handleRequest(
            handler,
            "GET",
            "/download/storage/v1/b/" + bucket + "/o/" + blobName + generateQueryString("ifGenerationMatch", ifGenerationMatch),
            BytesArray.EMPTY,
            range != null ? rangeHeader(range.start(), range.end()) : TestHttpExchange.EMPTY_HEADERS
        );
    }

    private static TestHttpResponse getBlobMetadata(
        GoogleCloudStorageHttpHandler handler,
        String bucket,
        String blobName,
        @Nullable Long ifGenerationMatch
    ) {
        return handleRequest(
            handler,
            "GET",
            "/storage/v1/b/" + bucket + "/o/" + blobName + generateQueryString("ifGenerationMatch", ifGenerationMatch)
        );
    }

    private static long getCurrentGeneration(GoogleCloudStorageHttpHandler handler, String bucket, String blobName) {
        final TestHttpResponse blobMetadata = getBlobMetadata(handler, bucket, blobName, null);
        assertEquals(RestStatus.OK, blobMetadata.restStatus());
        final Matcher matcher = GENERATION_PATTERN.matcher(blobMetadata.body.utf8ToString());
        assertTrue(matcher.find());
        return Long.parseLong(matcher.group(1));
    }

    private static TestHttpResponse listBlobs(GoogleCloudStorageHttpHandler handler, String bucket, String prefix, String delimiter) {
        return handleRequest(
            handler,
            "GET",
            "/storage/v1/b/" + bucket + "/o" + generateQueryString("prefix", prefix, "delimiter", delimiter)
        );
    }

    private record TestHttpResponse(int status, BytesReference body, Headers headers) {
        TestHttpResponse(RestStatus status, BytesReference body, Headers headers) {
            this(status.getStatus(), body, headers);
        }

        TestHttpResponse(RestStatus status, String body) {
            this(status.getStatus(), new BytesArray(body.getBytes(StandardCharsets.UTF_8)), TestHttpExchange.EMPTY_HEADERS);
        }

        TestHttpResponse(RestStatus status, Headers headers) {
            this(status.getStatus(), BytesArray.EMPTY, headers);
        }

        TestHttpResponse(int statusCode, Headers headers) {
            this(statusCode, BytesArray.EMPTY, headers);
        }

        RestStatus restStatus() {
            return requireNonNull(RestStatus.fromCode(status));
        }

        @Override
        public String toString() {
            return "TestHttpResponse{" + "status=" + status + ", body={size=" + body.utf8ToString() + "}, headers=" + headers + '}';
        }
    }

    private static TestHttpResponse handleRequest(GoogleCloudStorageHttpHandler handler, String method, String uri) {
        return handleRequest(handler, method, uri, "");
    }

    private static TestHttpResponse handleRequest(GoogleCloudStorageHttpHandler handler, String method, String uri, String requestBody) {
        return handleRequest(handler, method, uri, new BytesArray(requestBody.getBytes(StandardCharsets.UTF_8)));
    }

    private static TestHttpResponse handleRequest(
        GoogleCloudStorageHttpHandler handler,
        String method,
        String uri,
        String requestBody,
        Headers headers
    ) {
        return handleRequest(handler, method, uri, new BytesArray(requestBody.getBytes(StandardCharsets.UTF_8)), headers);
    }

    private static TestHttpResponse handleRequest(
        GoogleCloudStorageHttpHandler handler,
        String method,
        String uri,
        BytesReference requestBody
    ) {
        return handleRequest(handler, method, uri, requestBody, TestHttpExchange.EMPTY_HEADERS);
    }

    private static TestHttpResponse handleRequest(
        GoogleCloudStorageHttpHandler handler,
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
        final var responseHeaders = new Headers();
        httpExchange.getResponseHeaders().forEach((header, values) -> {
            // com.sun.net.httpserver.Headers.Headers() normalize keys
            if ("Range".equals(header) || "Content-range".equals(header) || "Location".equals(header)) {
                responseHeaders.put(header, List.copyOf(values));
            }
        });
        return new TestHttpResponse(httpExchange.getResponseCode(), httpExchange.getResponseBodyContents(), responseHeaders);
    }

    /**
     * Generate a query string for the given parameters
     *
     * @param parameters The query parameters as alternating key, value pairs
     * @return The query string including all parameters with a non-null value (e.g.
     */
    public static String generateQueryString(Object... parameters) {
        if (parameters.length % 2 != 0) {
            final String message = "Parameters must be represented as alternating key, value pairs";
            assert false : message;
            throw new IllegalArgumentException(message);
        }
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < parameters.length; i += 2) {
            final String key = String.valueOf(requireNonNull(parameters[i], "Parameter names must be non-null strings"));
            final Object value = parameters[i + 1];
            if (value != null) {
                if (builder.isEmpty() == false) {
                    builder.append("&");
                }
                builder.append(key).append("=").append(URLEncoder.encode(String.valueOf(value), StandardCharsets.UTF_8));
            }
        }
        if (builder.isEmpty() == false) {
            return "?" + builder;
        }
        return "";
    }

    private static Headers contentRangeHeader(@Nullable Integer startInclusive, @Nullable Integer endInclusive, @Nullable Integer limit) {
        final String rangeString = startInclusive != null && endInclusive != null ? startInclusive + "-" + endInclusive : "*";
        final String limitString = limit == null ? "*" : limit.toString();
        return Headers.of("Content-Range", "bytes " + rangeString + "/" + limitString);
    }

    private static Headers rangeHeader(long start, long end) {
        return Headers.of("Range", Strings.format("bytes=%d-%d", start, end));
    }

    private static String newMultipartBoundary() {
        return "__END_OF_PART__" + randomUUID();
    }

    private static BytesReference createGzipCompressedMultipartUploadBody(
        String bucketName,
        String path,
        BytesReference content,
        String boundary
    ) {
        final String metadataString = Strings.format("{\"bucket\":\"%s\", \"name\":\"%s\"}", bucketName, path);
        final String headerStr = """
            --$boundary
            Content-Length: $metadata-length
            Content-Type: application/json; charset=UTF-8
            content-transfer-encoding: binary

            $metadata
            --$boundary
            Content-Type: application/octet-stream
            content-transfer-encoding: binary

            """.replace("\n", "\r\n")
            .replace("$boundary", boundary)
            .replace("$metadata-length", Integer.toString(metadataString.length()))
            .replace("$metadata", metadataString);
        final BytesReference header = new BytesArray(headerStr.getBytes(StandardCharsets.UTF_8));
        final BytesReference footer = new BytesArray("""

            --$boundary--
            """.replace("\n", "\r\n").replace("$boundary", boundary));
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(out)) {
            gzipOutputStream.write(BytesReference.toBytes(CompositeBytesReference.of(header, content, footer)));
        } catch (IOException e) {
            fail(e);
        }
        return new BytesArray(out.toByteArray());
    }

    private static String createBatchDeleteRequest(String bucketName, String boundary, String... paths) {
        final String deleteRequestTemplate = """
            DELETE %s/storage/v1/b/%s/o/%s HTTP/1.1
            Authorization: Bearer foo
            x-goog-api-client: gl-java/23.0.0 gdcl/2.1.1 mac-os-x/15.2


            """;
        final String partTemplate = """
            --$boundary
            Content-Length: %d
            Content-Type: application/http
            content-id: %d
            content-transfer-encoding: binary

            %s
            """.replace("$boundary", boundary);
        StringBuilder builder = new StringBuilder();
        AtomicInteger contentId = new AtomicInteger();
        Arrays.stream(paths).forEach(p -> {
            final String deleteRequest = Strings.format(deleteRequestTemplate, HOST, bucketName, p);
            final String part = Strings.format(partTemplate, deleteRequest.length(), contentId.incrementAndGet(), deleteRequest);
            builder.append(part);
        });
        builder.append("--").append(boundary).append("--");
        return builder.toString();
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
            this.requestHeaders = new Headers(requestHeaders);
            this.requestHeaders.add("Host", HOST);
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
