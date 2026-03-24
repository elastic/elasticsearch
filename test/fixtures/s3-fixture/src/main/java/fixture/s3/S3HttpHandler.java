/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.XmlUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.fixture.HttpHeaderParser;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.test.fixture.HttpHeaderParser.parseRangeHeader;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.w3c.dom.Node.ELEMENT_NODE;

/**
 * Minimal HTTP handler that acts as a S3 compliant server
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
public class S3HttpHandler implements HttpHandler {

    private static final Logger logger = LogManager.getLogger(S3HttpHandler.class);

    private final String bucket;
    private final String basePath;
    private final String bucketAndBasePath;
    private final S3ConsistencyModel consistencyModel;
    private final Supplier<String> uuidGenerator;

    private final ConcurrentMap<String, BytesReference> blobs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MultipartUpload> uploads = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicInteger> completingUploads = new ConcurrentHashMap<>();

    public S3HttpHandler(final String bucket, S3ConsistencyModel consistencyModel) {
        this(bucket, null, consistencyModel);
    }

    public S3HttpHandler(final String bucket, @Nullable final String basePath, S3ConsistencyModel consistencyModel) {
        this.bucket = Objects.requireNonNull(bucket);
        this.basePath = Objects.requireNonNullElse(basePath, "");
        this.bucketAndBasePath = bucket + (Strings.hasText(basePath) ? "/" + basePath : "");
        this.consistencyModel = consistencyModel;
        // Per-thread random is based on the same seed so that they generate the same sequence of results across threads.
        // To ensure unique UUIDs across threads, we store and share a single random across threads so that each invocation
        // generates different UUIDs.
        final var random = new Random(ESTestCase.randomLong());
        this.uuidGenerator = () -> UUIDs.randomBase64UUID(random);
    }

    /**
     * Requests using these HTTP methods never have a request body (this is checked in the handler).
     */
    private static final Set<String> METHODS_HAVING_NO_REQUEST_BODY = Set.of("GET", "HEAD", "DELETE");

    private static final String SHA_256_ETAG_PREFIX = "es-test-sha-256-";

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        // Remove custom query parameters before processing the request. This simulates how S3 ignores them.
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html#LogFormatCustom
        final S3Request request = parseRequest(exchange);

        if (METHODS_HAVING_NO_REQUEST_BODY.contains(request.method())) {
            int read = exchange.getRequestBody().read();
            assert read == -1 : "Request body should have been empty but saw [" + read + "]";
        }

        try (exchange) {
            if (request.isHeadObjectRequest()) {
                final BytesReference blob = blobs.get(request.path());
                if (blob == null) {
                    exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                } else {
                    // HEAD response must include Content-Length header for S3 clients (AWS SDK) that read file size
                    exchange.getResponseHeaders().add("Content-Length", String.valueOf(blob.length()));
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                }
            } else if (request.isListMultipartUploadsRequest()) {

                final var prefix = request.getQueryParamOnce("prefix");
                assert Objects.requireNonNullElse(prefix, "").contains(basePath) : basePath + " vs " + request;

                final var uploadsList = new StringBuilder();
                uploadsList.append("<?xml version='1.0' encoding='UTF-8'?>");
                uploadsList.append("<ListMultipartUploadsResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>");
                uploadsList.append("<Bucket>").append(bucket).append("</Bucket>");
                uploadsList.append("<KeyMarker />");
                uploadsList.append("<UploadIdMarker />");
                uploadsList.append("<NextKeyMarker>--unused--</NextKeyMarker>");
                uploadsList.append("<NextUploadIdMarker />");
                uploadsList.append("<Delimiter />");
                uploadsList.append("<Prefix>").append(prefix).append("</Prefix>");
                uploadsList.append("<MaxUploads>10000</MaxUploads>");
                uploadsList.append("<IsTruncated>false</IsTruncated>");

                synchronized (uploads) {
                    for (final var multipartUpload : uploads.values()) {
                        if (multipartUpload.getPath().startsWith(prefix)) {
                            multipartUpload.appendXml(uploadsList);
                        }
                    }
                }

                uploadsList.append("</ListMultipartUploadsResult>");

                byte[] response = uploadsList.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (request.isInitiateMultipartUploadRequest()) {
                final var upload = putUpload(request.path().substring(bucket.length() + 2));
                final var uploadResult = new StringBuilder();
                uploadResult.append("<?xml version='1.0' encoding='UTF-8'?>");
                uploadResult.append("<InitiateMultipartUploadResult>");
                uploadResult.append("<Bucket>").append(bucket).append("</Bucket>");
                uploadResult.append("<Key>").append(upload.getPath()).append("</Key>");
                uploadResult.append("<UploadId>").append(upload.getUploadId()).append("</UploadId>");
                uploadResult.append("</InitiateMultipartUploadResult>");

                byte[] response = uploadResult.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (request.isUploadPartRequest()) {
                final var upload = getUpload(request.getQueryParamOnce("uploadId"));
                if (upload == null) {
                    exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                } else {
                    // CopyPart is UploadPart with an x-amz-copy-source header
                    final var copySource = copySourceName(exchange);
                    if (copySource != null) {
                        var sourceBlob = blobs.get(copySource);
                        if (sourceBlob == null) {
                            exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                        } else {
                            var range = parsePartRange(exchange);
                            if (range.end() == null) {
                                throw new AssertionError("Copy-part range must specify an end: " + range);
                            }
                            int start = Math.toIntExact(range.start());
                            int len = Math.toIntExact(range.end() - range.start() + 1);
                            var part = sourceBlob.slice(start, len);
                            var etag = getEtagFromContents(part);
                            upload.addPart(etag, part);
                            byte[] response = ("""
                                <?xml version="1.0" encoding="UTF-8"?>
                                <CopyPartResult>
                                    <ETag>%s</ETag>
                                </CopyPartResult>""".formatted(etag)).getBytes(StandardCharsets.UTF_8);
                            exchange.getResponseHeaders().add("Content-Type", "application/xml");
                            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                            exchange.getResponseBody().write(response);
                        }
                    } else {
                        final Tuple<String, BytesReference> blob = parseRequestBody(exchange);
                        upload.addPart(blob.v1(), blob.v2());
                        exchange.getResponseHeaders().add("ETag", blob.v1());
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                    }
                }

            } else if (request.isCompleteMultipartUploadRequest()) {
                final var uploadId = request.getQueryParamOnce("uploadId");
                final byte[] responseBody;
                final RestStatus responseCode;
                try (var ignoredCompletingUploadRef = setUploadCompleting(uploadId)) {
                    synchronized (uploads) {
                        final var upload = getUpload(request.getQueryParamOnce("uploadId"));
                        if (upload == null) {
                            if (Randomness.get().nextBoolean()) {
                                responseCode = RestStatus.NOT_FOUND;
                                responseBody = null;
                            } else {
                                responseCode = RestStatus.OK;
                                responseBody = """
                                    <?xml version="1.0" encoding="UTF-8"?>
                                    <Error>
                                    <Code>NoSuchUpload</Code>
                                    <Message>No such upload</Message>
                                    <RequestId>test-request-id</RequestId>
                                    <HostId>test-host-id</HostId>
                                    </Error>""".getBytes(StandardCharsets.UTF_8);
                            }
                        } else {
                            final var blobContents = upload.complete(extractPartEtags(Streams.readFully(exchange.getRequestBody())));
                            responseCode = updateBlobContents(exchange, request.path(), blobContents);
                            if (responseCode == RestStatus.OK) {
                                responseBody = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                    + "<CompleteMultipartUploadResult>\n"
                                    + "<Bucket>"
                                    + bucket
                                    + "</Bucket>\n"
                                    + "<Key>"
                                    + request.path()
                                    + "</Key>\n"
                                    + "</CompleteMultipartUploadResult>").getBytes(StandardCharsets.UTF_8);
                            } else {
                                responseBody = null;
                            }
                            if (responseCode != RestStatus.PRECONDITION_FAILED) {
                                removeUpload(upload.getUploadId());
                            }
                        }
                    }
                }
                if (responseCode == RestStatus.OK) {
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), responseBody.length);
                    exchange.getResponseBody().write(responseBody);
                } else {
                    exchange.sendResponseHeaders(responseCode.getStatus(), -1);
                }

            } else if (request.isAbortMultipartUploadRequest()) {
                final var uploadId = request.getQueryParamOnce("uploadId");
                if (consistencyModel.hasStrongMultipartUploads() == false && completingUploads.containsKey(uploadId)) {
                    // See AWS support case 176070774900712: aborts may sometimes return early if complete is already in progress
                    exchange.sendResponseHeaders(RestStatus.NO_CONTENT.getStatus(), -1);
                } else {
                    final var upload = removeUpload(request.getQueryParamOnce("uploadId"));
                    exchange.sendResponseHeaders((upload == null ? RestStatus.NOT_FOUND : RestStatus.NO_CONTENT).getStatus(), -1);
                }

            } else if (request.isPutObjectRequest()) {
                // a copy request is a put request with an X-amz-copy-source header
                final var copySource = copySourceName(exchange);
                if (copySource != null) {
                    if (isProtectOverwrite(exchange)) {
                        throw new AssertionError("If-None-Match: * header is not supported here");
                    }
                    if (getRequiredExistingETag(exchange) != null) {
                        throw new AssertionError("If-Match: * header is not supported here");
                    }

                    var sourceBlob = blobs.get(copySource);
                    if (sourceBlob == null) {
                        exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                    } else {
                        blobs.put(request.path(), sourceBlob);

                        byte[] response = ("""
                            <?xml version="1.0" encoding="UTF-8"?>
                            <CopyObjectResult></CopyObjectResult>""").getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().add("Content-Type", "application/xml");
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                        exchange.getResponseBody().write(response);
                    }
                } else {
                    final Tuple<String, BytesReference> blob = parseRequestBody(exchange);
                    final var updateResponseCode = updateBlobContents(exchange, request.path(), blob.v2());

                    if (updateResponseCode == RestStatus.OK) {
                        exchange.getResponseHeaders().add("ETag", blob.v1());
                    }
                    exchange.sendResponseHeaders(updateResponseCode.getStatus(), -1);
                }

            } else if (request.isListObjectsRequest()) {
                final StringBuilder list = new StringBuilder();
                list.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                list.append("<ListBucketResult>");
                final String prefix = request.getOptionalQueryParam("prefix").orElse(null);
                if (prefix != null) {
                    list.append("<Prefix>").append(prefix).append("</Prefix>");
                }
                final Set<String> commonPrefixes = new HashSet<>();
                final String delimiter = request.getOptionalQueryParam("delimiter").orElse(null);
                if (delimiter != null) {
                    list.append("<Delimiter>").append(delimiter).append("</Delimiter>");
                }
                // Would be good to test pagination here (the only real difference between ListObjects and ListObjectsV2) but for now
                // we return all the results at once.
                list.append("<IsTruncated>false</IsTruncated>");
                for (Map.Entry<String, BytesReference> blob : blobs.entrySet()) {
                    if (prefix != null && blob.getKey().startsWith("/" + bucket + "/" + prefix) == false) {
                        continue;
                    }
                    String blobPath = blob.getKey().replace("/" + bucket + "/", "");
                    if (delimiter != null) {
                        int fromIndex = (prefix != null ? prefix.length() : 0);
                        int delimiterPosition = blobPath.indexOf(delimiter, fromIndex);
                        if (delimiterPosition > 0) {
                            commonPrefixes.add(blobPath.substring(0, delimiterPosition) + delimiter);
                            continue;
                        }
                    }
                    list.append("<Contents>");
                    list.append("<Key>").append(blobPath).append("</Key>");
                    list.append("<Size>").append(blob.getValue().length()).append("</Size>");
                    list.append("</Contents>");
                }
                commonPrefixes.forEach(
                    commonPrefix -> list.append("<CommonPrefixes><Prefix>").append(commonPrefix).append("</Prefix></CommonPrefixes>")
                );
                list.append("</ListBucketResult>");

                byte[] response = list.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (request.isGetObjectRequest()) {
                final BytesReference blob = blobs.get(request.path());
                if (blob == null) {
                    exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                    return;
                }

                final String etagFromContents = getEtagFromContents(blob);
                final String ifMatchHeader = exchange.getRequestHeaders().getFirst("If-Match");
                if (ifMatchHeader != null && ifMatchHeader.equals("*") == false) {
                    if (etagFromContents.equals(ifMatchHeader) == false) {
                        final String response = Strings.format("""
                            <?xml version="1.0" encoding="UTF-8"?>
                            <Error>
                                <Code>PreconditionFailed</Code>
                                <Message>At least one of the pre-conditions you specified did not hold</Message>
                                <Condition>If-Match</Condition>
                                <RequestId>test-request-id</RequestId>
                                <HostId>test-host-id</HostId>
                            </Error>""");
                        exchange.getResponseHeaders().add("Content-Type", "application/xml");
                        exchange.sendResponseHeaders(RestStatus.PRECONDITION_FAILED.getStatus(), response.length());
                        exchange.getResponseBody().write(response.getBytes(StandardCharsets.UTF_8));
                        return;
                    }
                }

                exchange.getResponseHeaders().add("ETag", etagFromContents);
                final String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
                if (rangeHeader == null) {
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), blob.length());
                    blob.writeTo(exchange.getResponseBody());
                    return;
                }

                // S3 supports https://www.rfc-editor.org/rfc/rfc9110.html#name-range
                // This handler supports both bounded ranges (bytes=0-100) and open-ended ranges (bytes=100-)
                final HttpHeaderParser.Range range = parseRangeHeader(rangeHeader);
                if (range == null) {
                    throw new AssertionError("Bytes range does not match expected pattern: " + rangeHeader);
                }
                long start = range.start();
                // For open-ended ranges (bytes=N-), end is null, meaning "to end of file"
                long end = range.end() != null ? range.end() : blob.length() - 1;
                if (end < start) {
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), blob.length());
                    blob.writeTo(exchange.getResponseBody());
                    return;
                } else if (blob.length() <= start) {
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.sendResponseHeaders(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus(), -1);
                    return;
                }
                var responseBlob = blob.slice(Math.toIntExact(start), Math.toIntExact(Math.min(end - start + 1, blob.length() - start)));
                end = start + responseBlob.length() - 1;
                exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                exchange.getResponseHeaders().add("Content-Range", String.format(Locale.ROOT, "bytes %d-%d/%d", start, end, blob.length()));
                exchange.sendResponseHeaders(RestStatus.PARTIAL_CONTENT.getStatus(), responseBlob.length());
                responseBlob.writeTo(exchange.getResponseBody());

            } else if (request.isDeleteObjectRequest()) {
                int deletions = 0;
                for (Iterator<Map.Entry<String, BytesReference>> iterator = blobs.entrySet().iterator(); iterator.hasNext();) {
                    Map.Entry<String, BytesReference> blob = iterator.next();
                    if (blob.getKey().startsWith(request.path())) {
                        iterator.remove();
                        deletions++;
                    }
                }
                exchange.sendResponseHeaders((deletions > 0 ? RestStatus.OK : RestStatus.NO_CONTENT).getStatus(), -1);

            } else if (request.isMultiObjectDeleteRequest()) {
                final String requestBody = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), UTF_8));

                final StringBuilder deletes = new StringBuilder();
                deletes.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                deletes.append("<DeleteResult>");
                for (Iterator<Map.Entry<String, BytesReference>> iterator = blobs.entrySet().iterator(); iterator.hasNext();) {
                    Map.Entry<String, BytesReference> blob = iterator.next();
                    String key = blob.getKey().replace("/" + bucket + "/", "");
                    if (requestBody.contains("<Key>" + key + "</Key>")) {
                        deletes.append("<Deleted><Key>").append(key).append("</Key></Deleted>");
                        iterator.remove();
                    }
                }
                deletes.append("</DeleteResult>");

                byte[] response = deletes.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else {
                logger.error("unknown request: {}", request);
                exchange.sendResponseHeaders(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), -1);
            }
        } catch (Exception e) {
            logger.error("exception in request " + request, e);
            throw e;
        }
    }

    /**
     * Update the blob contents if and only if the preconditions in the request are satisfied.
     *
     * @return {@link RestStatus#OK} if the blob contents were updated, or else a different status code to indicate the error: possibly
     *         {@link RestStatus#CONFLICT} in any case, but if not then either  {@link RestStatus#PRECONDITION_FAILED} if the object exists
     *         but doesn't match the specified precondition, or {@link RestStatus#NOT_FOUND} if the object doesn't exist but is required to
     *         do so by the precondition.
     *
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html#conditional-error-response">AWS docs</a>
     */
    private RestStatus updateBlobContents(HttpExchange exchange, String path, BytesReference newContents) {
        if (consistencyModel.hasConditionalWrites()) {
            if (isProtectOverwrite(exchange)) {
                return blobs.putIfAbsent(path, newContents) == null
                    ? RestStatus.OK
                    : ESTestCase.randomFrom(RestStatus.PRECONDITION_FAILED, RestStatus.CONFLICT);
            }

            final var requireExistingETag = getRequiredExistingETag(exchange);
            if (requireExistingETag != null) {
                final var responseCode = new AtomicReference<>(RestStatus.OK);
                blobs.compute(path, (ignoredPath, existingContents) -> {
                    if (existingContents != null && requireExistingETag.equals(getEtagFromContents(existingContents))) {
                        return newContents;
                    }

                    responseCode.set(
                        ESTestCase.randomFrom(
                            existingContents == null ? RestStatus.NOT_FOUND : RestStatus.PRECONDITION_FAILED,
                            RestStatus.CONFLICT
                        )
                    );
                    return existingContents;
                });
                return responseCode.get();
            }
        }

        blobs.put(path, newContents);
        return RestStatus.OK;
    }

    /**
     * Etags are opaque identifiers for the contents of an object.
     *
     * @see <a href="https://en.wikipedia.org/wiki/HTTP_ETag">HTTP ETag on Wikipedia</a>.
     */
    public static String getEtagFromContents(BytesReference blobContents) {
        return '"' + SHA_256_ETAG_PREFIX + MessageDigests.toHexString(MessageDigests.digest(blobContents, MessageDigests.sha256())) + '"';
    }

    public Map<String, BytesReference> blobs() {
        return blobs;
    }

    private static final Pattern chunkSignaturePattern = Pattern.compile("^([0-9a-z]+);chunk-signature=([^\\r\\n]*)$");

    private static Tuple<String, BytesReference> parseRequestBody(final HttpExchange exchange) throws IOException {
        try {
            final BytesReference bytesReference;

            final String headerDecodedContentLength = exchange.getRequestHeaders().getFirst("x-amz-decoded-content-length");
            if (headerDecodedContentLength == null) {
                bytesReference = Streams.readFully(exchange.getRequestBody());
            } else {
                BytesReference requestBody = Streams.readFully(exchange.getRequestBody());
                int chunkIndex = 0;
                final List<BytesReference> chunks = new ArrayList<>();

                while (true) {
                    chunkIndex += 1;

                    final int headerLength = requestBody.indexOf((byte) '\n', 0) + 1; // includes terminating \r\n
                    if (headerLength == 0) {
                        throw new IllegalStateException("header of chunk [" + chunkIndex + "] was not terminated");
                    }
                    if (headerLength > 150) {
                        throw new IllegalStateException(
                            "header of chunk [" + chunkIndex + "] was too long at [" + headerLength + "] bytes"
                        );
                    }
                    if (headerLength < 3) {
                        throw new IllegalStateException(
                            "header of chunk [" + chunkIndex + "] was too short at [" + headerLength + "] bytes"
                        );
                    }
                    if (requestBody.get(headerLength - 1) != '\n' || requestBody.get(headerLength - 2) != '\r') {
                        throw new IllegalStateException("header of chunk [" + chunkIndex + "] not terminated with [\\r\\n]");
                    }

                    final String header = requestBody.slice(0, headerLength - 2).utf8ToString();
                    final Matcher matcher = chunkSignaturePattern.matcher(header);
                    if (matcher.find() == false) {
                        throw new IllegalStateException(
                            "header of chunk [" + chunkIndex + "] did not match expected pattern: [" + header + "]"
                        );
                    }
                    final int chunkSize = Integer.parseUnsignedInt(matcher.group(1), 16);

                    if (requestBody.get(headerLength + chunkSize) != '\r' || requestBody.get(headerLength + chunkSize + 1) != '\n') {
                        throw new IllegalStateException("chunk [" + chunkIndex + "] not terminated with [\\r\\n]");
                    }

                    if (chunkSize != 0) {
                        chunks.add(requestBody.slice(headerLength, chunkSize));
                    }

                    final int toSkip = headerLength + chunkSize + 2;
                    requestBody = requestBody.slice(toSkip, requestBody.length() - toSkip);

                    if (chunkSize == 0) {
                        break;
                    }
                }

                bytesReference = CompositeBytesReference.of(chunks.toArray(new BytesReference[0]));

                if (bytesReference.length() != Integer.parseInt(headerDecodedContentLength)) {
                    throw new IllegalStateException(
                        "Something went wrong when parsing the chunked request "
                            + "[bytes read="
                            + bytesReference.length()
                            + ", expected="
                            + headerDecodedContentLength
                            + "]"
                    );
                }
            }
            return Tuple.tuple(getEtagFromContents(bytesReference), bytesReference);
        } catch (Exception e) {
            logger.error("exception in parseRequestBody", e);
            exchange.sendResponseHeaders(500, 0);
            try (PrintStream printStream = new PrintStream(exchange.getResponseBody())) {
                printStream.println(e);
                e.printStackTrace(printStream);
            }
            throw e;
        }
    }

    static List<String> extractPartEtags(BytesReference completeMultipartUploadBody) {
        try {
            final var document = XmlUtils.getHardenedBuilderFactory().newDocumentBuilder().parse(completeMultipartUploadBody.streamInput());
            final var parts = document.getElementsByTagName("Part");
            final var result = new ArrayList<String>(parts.getLength());
            for (int partIndex = 0; partIndex < parts.getLength(); partIndex++) {
                final var part = parts.item(partIndex);
                String etag = null;
                int partNumber = -1;
                final var childNodes = part.getChildNodes();
                for (int childIndex = 0; childIndex < childNodes.getLength(); childIndex++) {
                    final var childNode = childNodes.item(childIndex);
                    if (childNode.getNodeType() == ELEMENT_NODE) {
                        if (childNode.getNodeName().equals("ETag")) {
                            etag = childNode.getTextContent();
                        } else if (childNode.getNodeName().equals("PartNumber")) {
                            partNumber = Integer.parseInt(childNode.getTextContent()) - 1;
                        }
                    }
                }

                if (etag == null || partNumber == -1) {
                    throw new IllegalStateException("incomplete part details");
                }

                while (result.size() <= partNumber) {
                    result.add(null);
                }

                if (result.get(partNumber) != null) {
                    throw new IllegalStateException("duplicate part found");
                }
                result.set(partNumber, etag);
            }

            if (result.stream().anyMatch(Objects::isNull)) {
                throw new IllegalStateException("missing part");
            }

            return result;
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    @Nullable // if no X-amz-copy-source header present
    private static String copySourceName(final HttpExchange exchange) {
        final var copySources = exchange.getRequestHeaders().get("X-amz-copy-source");
        if (copySources != null) {
            if (copySources.size() != 1) {
                throw new AssertionError("multiple X-amz-copy-source headers found: " + copySources);
            }
            final var copySource = copySources.get(0);
            // SDKv1 uses format /bucket/path/blob whereas SDKv2 omits the leading / so we must add it back in
            return copySource.length() > 0 && copySource.charAt(0) == '/' ? copySource : ("/" + copySource);
        } else {
            return null;
        }
    }

    private static HttpHeaderParser.Range parsePartRange(final HttpExchange exchange) {
        final var sourceRangeHeaders = exchange.getRequestHeaders().get("X-amz-copy-source-range");
        if (sourceRangeHeaders == null) {
            throw new IllegalStateException("missing x-amz-copy-source-range header");
        }
        if (sourceRangeHeaders.size() != 1) {
            throw new IllegalStateException("expected 1 x-amz-copy-source-range header, found " + sourceRangeHeaders.size());
        }
        return parseRangeHeader(sourceRangeHeaders.getFirst());
    }

    private static boolean isProtectOverwrite(final HttpExchange exchange) {
        final var ifNoneMatch = exchange.getRequestHeaders().get("If-None-Match");

        if (ifNoneMatch == null) {
            return false;
        }

        if (exchange.getRequestHeaders().get("If-Match") != null) {
            throw new AssertionError("Handling both If-None-Match and If-Match headers is not supported");
        }

        if (ifNoneMatch.size() != 1) {
            throw new AssertionError("multiple If-None-Match headers found: " + ifNoneMatch);
        }

        if (ifNoneMatch.getFirst().equals("*")) {
            return true;
        }

        throw new AssertionError("invalid If-None-Match header: " + ifNoneMatch);
    }

    @Nullable // if no If-Match header found
    private static String getRequiredExistingETag(final HttpExchange exchange) {
        final var ifMatch = exchange.getRequestHeaders().get("If-Match");

        if (ifMatch == null) {
            return null;
        }

        if (exchange.getRequestHeaders().get("If-None-Match") != null) {
            throw new AssertionError("Handling both If-None-Match and If-Match headers is not supported");
        }

        final var iterator = ifMatch.iterator();
        if (iterator.hasNext()) {
            final var result = iterator.next();
            if (iterator.hasNext() == false) {
                return result;
            }
        }

        throw new AssertionError("multiple If-Match headers found: " + ifMatch);
    }

    MultipartUpload putUpload(String path) {
        final var upload = new MultipartUpload(uuidGenerator.get(), path);
        synchronized (uploads) {
            assertNull("upload " + upload.getUploadId() + " should not exist", uploads.put(upload.getUploadId(), upload));
            return upload;
        }
    }

    MultipartUpload getUpload(String uploadId) {
        synchronized (uploads) {
            return uploads.get(uploadId);
        }
    }

    MultipartUpload removeUpload(String uploadId) {
        synchronized (uploads) {
            return uploads.remove(uploadId);
        }
    }

    private Releasable setUploadCompleting(String uploadId) {
        completingUploads.computeIfAbsent(uploadId, ignored -> new AtomicInteger()).incrementAndGet();
        return () -> clearUploadCompleting(uploadId);
    }

    private void clearUploadCompleting(String uploadId) {
        completingUploads.compute(uploadId, (ignored, uploadCount) -> {
            if (uploadCount == null) {
                throw new AssertionError("upload [" + uploadId + "] not tracked");
            }
            if (uploadCount.decrementAndGet() == 0) {
                return null;
            } else {
                return uploadCount;
            }
        });
    }

    public S3Request parseRequest(HttpExchange exchange) {
        final String queryString = exchange.getRequestURI().getQuery();
        final Map<String, List<String>> queryParameters;
        if (Strings.hasText(queryString)) {
            queryParameters = new HashMap<>();
            for (final String queryPart : queryString.split("&")) {
                final String paramName, paramValue;
                final int equalsPos = queryPart.indexOf('=');
                if (equalsPos == -1) {
                    paramName = queryPart;
                    paramValue = null;
                } else {
                    paramName = queryPart.substring(0, equalsPos);
                    paramValue = queryPart.substring(equalsPos + 1);
                }
                queryParameters.computeIfAbsent(paramName, ignored -> new ArrayList<>()).add(paramValue);
            }
        } else {
            queryParameters = Map.of();
        }

        return new S3Request(exchange.getRequestMethod(), exchange.getRequestURI().getPath(), queryParameters);
    }

    public class S3Request {
        private final String method;
        private final String path;
        private final Map<String, List<String>> queryParameters;

        public S3Request(String method, String path, Map<String, List<String>> queryParameters) {
            this.method = method;
            this.path = path;
            this.queryParameters = queryParameters;
        }

        public String method() {
            return method;
        }

        public String path() {
            return path;
        }

        public Map<String, List<String>> queryParameters() {
            return queryParameters;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (S3Request) obj;
            return Objects.equals(this.method, that.method)
                && Objects.equals(this.path, that.path)
                && Objects.equals(this.queryParameters, that.queryParameters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(method, path, queryParameters);
        }

        @Override
        public String toString() {
            return Strings.format("RequestComponents[method=%s, path=%s, queryParameters=%s]", method, path, queryParameters);
        }

        public boolean hasQueryParamOnce(String name) {
            final var values = queryParameters.get(name);
            return values != null && values.size() == 1;
        }

        public String getQueryParamOnce(String name) {
            final var values = queryParameters.get(name);
            assertNotNull(name, values);
            assertEquals(name + "=" + values, 1, values.size());
            return values.get(0);
        }

        public Optional<String> getOptionalQueryParam(String name) {
            final var values = queryParameters.get(name);
            if (values == null) {
                return Optional.empty();
            }
            assertEquals(name + "=" + values, 1, values.size());
            return Optional.of(values.get(0));
        }

        private boolean isBucketRootPath() {
            return path.equals("/" + bucket) || path.equals("/" + bucket + "/");
        }

        private boolean isUnderBucketRootAndBasePath() {
            return path.startsWith("/" + bucketAndBasePath + "/");
        }

        public boolean isHeadObjectRequest() {
            return "HEAD".equals(method) && isUnderBucketRootAndBasePath();
        }

        public boolean isListMultipartUploadsRequest() {
            return "GET".equals(method)
                && isBucketRootPath()
                && hasQueryParamOnce("uploads")
                && getQueryParamOnce("uploads") == null
                && hasQueryParamOnce("prefix");
        }

        public boolean isInitiateMultipartUploadRequest() {
            return "POST".equals(method)
                && isUnderBucketRootAndBasePath()
                && hasQueryParamOnce("uploads")
                && getQueryParamOnce("uploads") == null;
        }

        public boolean isUploadPartRequest() {
            return "PUT".equals(method)
                && isUnderBucketRootAndBasePath()
                && hasQueryParamOnce("uploadId")
                && getQueryParamOnce("uploadId") != null
                && hasQueryParamOnce("partNumber");
        }

        public boolean isCompleteMultipartUploadRequest() {
            return "POST".equals(method)
                && isUnderBucketRootAndBasePath()
                && hasQueryParamOnce("uploadId")
                && getQueryParamOnce("uploadId") != null;
        }

        public boolean isAbortMultipartUploadRequest() {
            return "DELETE".equals(method)
                && isUnderBucketRootAndBasePath()
                && hasQueryParamOnce("uploadId")
                && getQueryParamOnce("uploadId") != null;
        }

        public boolean isPutObjectRequest() {
            return "PUT".equals(method) && isUnderBucketRootAndBasePath() && queryParameters.containsKey("uploadId") == false;
        }

        public boolean isGetObjectRequest() {
            return "GET".equals(method) && isUnderBucketRootAndBasePath();
        }

        public boolean isDeleteObjectRequest() {
            return "DELETE".equals(method) && isUnderBucketRootAndBasePath();
        }

        public boolean isListObjectsRequest() {
            return "GET".equals(method) && isBucketRootPath() && hasQueryParamOnce("prefix") && hasQueryParamOnce("uploads") == false;
        }

        public boolean isMultiObjectDeleteRequest() {
            return "POST".equals(method) && isBucketRootPath() && hasQueryParamOnce("delete") && getQueryParamOnce("delete") == null;
        }

    }
}
