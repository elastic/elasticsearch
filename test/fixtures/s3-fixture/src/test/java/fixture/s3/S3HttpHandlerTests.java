/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package fixture.s3;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;

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
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.greaterThan;

public class S3HttpHandlerTests extends ESTestCase {

    public void testRejectsBadUri() {
        assertEquals(
            RestStatus.INTERNAL_SERVER_ERROR,
            handleRequest(new S3HttpHandler("bucket", "path"), randomFrom("GET", "PUT", "POST", "DELETE", "HEAD"), "/not-in-bucket")
                .status()
        );
    }

    public void testSimpleObjectOperations() {
        final var handler = new S3HttpHandler("bucket", "path");

        assertEquals(RestStatus.NOT_FOUND, handleRequest(handler, "GET", "/bucket/path/blob").status());

        assertEquals(
            new TestHttpResponse(RestStatus.OK, """
                <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix></Prefix></ListBucketResult>"""),
            handleRequest(handler, "GET", "/bucket/?prefix=")
        );

        final var body = randomAlphaOfLength(50);
        assertEquals(RestStatus.OK, handleRequest(handler, "PUT", "/bucket/path/blob", body).status());
        assertEquals(new TestHttpResponse(RestStatus.OK, body), handleRequest(handler, "GET", "/bucket/path/blob"));

        assertEquals(new TestHttpResponse(RestStatus.OK, """
            <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix></Prefix>\
            <Contents><Key>path/blob</Key><Size>50</Size></Contents>\
            </ListBucketResult>"""), handleRequest(handler, "GET", "/bucket/?prefix="));

        assertEquals(new TestHttpResponse(RestStatus.OK, """
            <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix>path/</Prefix>\
            <Contents><Key>path/blob</Key><Size>50</Size></Contents>\
            </ListBucketResult>"""), handleRequest(handler, "GET", "/bucket/?prefix=path/"));

        assertEquals(
            new TestHttpResponse(RestStatus.OK, """
                <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix>path/other</Prefix></ListBucketResult>"""),
            handleRequest(handler, "GET", "/bucket/?prefix=path/other")
        );

        assertEquals(RestStatus.OK, handleRequest(handler, "DELETE", "/bucket/path/blob").status());
        assertEquals(RestStatus.NO_CONTENT, handleRequest(handler, "DELETE", "/bucket/path/blob").status());

        assertEquals(
            new TestHttpResponse(RestStatus.OK, """
                <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix></Prefix></ListBucketResult>"""),
            handleRequest(handler, "GET", "/bucket/?prefix=")
        );
    }

    public void testSingleMultipartUpload() {
        final var handler = new S3HttpHandler("bucket", "path");

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
        assertEquals(new TestHttpResponse(RestStatus.OK, "", part1Etag), uploadPart1Response);

        final var part2 = randomAlphaOfLength(50);
        final var uploadPart2Response = handleRequest(handler, "PUT", "/bucket/path/blob?uploadId=" + uploadId + "&partNumber=2", part2);
        final var part2Etag = Objects.requireNonNull(uploadPart2Response.etag());
        assertEquals(new TestHttpResponse(RestStatus.OK, "", part2Etag), uploadPart2Response);

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

        assertEquals(new TestHttpResponse(RestStatus.OK, """
            <?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Prefix></Prefix>\
            <Contents><Key>path/blob</Key><Size>100</Size></Contents>\
            </ListBucketResult>"""), handleRequest(handler, "GET", "/bucket/?prefix="));

        assertEquals(new TestHttpResponse(RestStatus.OK, part1 + part2), handleRequest(handler, "GET", "/bucket/path/blob"));

        assertEquals(new TestHttpResponse(RestStatus.OK, """
            <?xml version='1.0' encoding='UTF-8'?>\
            <ListMultipartUploadsResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>\
            <Bucket>bucket</Bucket><KeyMarker /><UploadIdMarker /><NextKeyMarker>--unused--</NextKeyMarker><NextUploadIdMarker />\
            <Delimiter /><Prefix>path/blob</Prefix><MaxUploads>10000</MaxUploads><IsTruncated>false</IsTruncated>\
            </ListMultipartUploadsResult>"""), handleRequest(handler, "GET", "/bucket/?uploads&prefix=path/blob"));
    }

    public void testListAndAbortMultipartUpload() {
        final var handler = new S3HttpHandler("bucket", "path");

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
        assertEquals(new TestHttpResponse(RestStatus.OK, "", part1Etag), uploadPart1Response);

        final var part2 = randomAlphaOfLength(50);
        final var uploadPart2Response = handleRequest(handler, "PUT", "/bucket/path/blob?uploadId=" + uploadId + "&partNumber=2", part2);
        final var part2Etag = Objects.requireNonNull(uploadPart2Response.etag());
        assertEquals(new TestHttpResponse(RestStatus.OK, "", part2Etag), uploadPart2Response);

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

        assertEquals(RestStatus.NOT_FOUND, handleRequest(handler, "POST", "/bucket/path/blob?uploadId=" + uploadId, Strings.format("""
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
            </CompleteMultipartUpload>""", part1Etag, part2Etag)).status());
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

    private void runExtractPartETagsTest(String body, String... expectedTags) {
        assertEquals(List.of(expectedTags), S3HttpHandler.extractPartEtags(new BytesArray(body.getBytes(StandardCharsets.UTF_8))));
    }

    private record TestHttpResponse(RestStatus status, String body, @Nullable String etag) {
        TestHttpResponse(RestStatus status, String body) {
            this(status, body, null);
        }
    }

    private static TestHttpResponse handleRequest(S3HttpHandler handler, String method, String uri) {
        return handleRequest(handler, method, uri, "");
    }

    private static TestHttpResponse handleRequest(S3HttpHandler handler, String method, String uri, String requestBody) {
        final var httpExchange = new TestHttpExchange(method, uri, new BytesArray(requestBody.getBytes(StandardCharsets.UTF_8)));
        try {
            handler.handle(httpExchange);
        } catch (IOException e) {
            throw new AssertionError("unexpected", e);
        }
        assertNotEquals(0, httpExchange.getResponseCode());
        return new TestHttpResponse(
            RestStatus.fromCode(httpExchange.getResponseCode()),
            httpExchange.getResponseBodyContents().utf8ToString(),
            httpExchange.getResponseHeaders().getFirst("ETag")
        );
    }

    private static class TestHttpExchange extends HttpExchange {

        private static final Headers EMPTY_HEADERS = new Headers();

        private final String method;
        private final URI uri;
        private final BytesReference requestBody;

        private final Headers responseHeaders = new Headers();
        private final BytesStreamOutput responseBody = new BytesStreamOutput();
        private int responseCode;

        TestHttpExchange(String method, String uri, BytesReference requestBody) {
            this.method = method;
            this.uri = URI.create(uri);
            this.requestBody = requestBody;
        }

        @Override
        public Headers getRequestHeaders() {
            return EMPTY_HEADERS;
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
