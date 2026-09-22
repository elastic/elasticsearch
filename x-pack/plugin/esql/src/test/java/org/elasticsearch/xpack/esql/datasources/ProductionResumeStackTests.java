/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec;
import org.elasticsearch.xpack.esql.datasource.http.HttpConfiguration;
import org.elasticsearch.xpack.esql.datasource.http.HttpStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalObjectChangedException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end coverage of the mid-read fail-closed behaviour on the stack a query actually builds,
 * rather than on {@link RetryableStorageObject} over a hand-rolled delegate.
 * <p>
 * A compressed file split is read through
 * {@code DecompressingStorageObject(RangeStorageObject(RetryableStorageObject(provider)))}: the split's
 * listing span — not the provider's cached object size — is what {@code RangeStorageObject} passes down
 * as the range length, so it is the span that defines the expected byte count the resume layer holds the
 * body to. These tests exercise that composition with a real provider ({@link HttpStorageObject}, which
 * sends and enforces {@code If-Match}) and a real codec.
 */
public class ProductionResumeStackTests extends ESTestCase {

    private static final StoragePath PATH = StoragePath.of("https://example.com/2026.csv.gz");
    private static final String ETAG = "\"gen-1\"";
    private static final int PARTIAL_CONTENT = 206;
    private static final int PRECONDITION_FAILED = 412;

    /**
     * A rewrite that lands between the first open and the resume: the provider's {@code If-Match} answers
     * 412, which must surface as {@link ExternalObjectChangedException} (503) instead of being retried
     * against a generation that will keep answering 412 until the budget is spent.
     */
    public void testMidReadRewriteThroughRetryableAndProviderIsObjectChanged() throws Exception {
        byte[] body = randomByteArrayOfLength(64);
        HttpClient client = mock(HttpClient.class);
        stubSends(
            client,
            List.of(
                // First open delivers a prefix, then the connection drops.
                rangeResponse(body, 0, body.length, new PrefixThenFaultStream(body, 20, new SocketException("Connection reset"))),
                // The If-Match'd resume finds a different generation.
                statusResponse(PRECONDITION_FAILED)
            )
        );

        StorageObject provider = new HttpStorageObject(client, PATH, HttpConfiguration.defaults());
        StorageObject retryable = new RetryableStorageObject(provider, new RetryPolicy(3, 1, 10));

        try (InputStream in = retryable.newStream(0, body.length)) {
            ExternalObjectChangedException thrown = expectThrows(ExternalObjectChangedException.class, in::readAllBytes);
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(thrown));
        }
        assertEquals("the 412 must not be retried: exactly one open plus one resume", 2, sentIfMatchHeaders.size());
        assertNull("first open is unpinned", sentIfMatchHeaders.get(0));
        assertEquals("the resume carries the pin", ETAG, sentIfMatchHeaders.get(1));
    }

    /**
     * A transport drop inside a {@code .csv.gz} split: the split span says more bytes are owed, so the
     * resume completes the compressed body and the decompressed CSV is delivered intact. Without the
     * resume the gzip trailer would be missing and the query would fail.
     */
    public void testTruncatedTransferInsideGzipSplitResumesAndDecompresses() throws Exception {
        byte[] csv = csvPayload();
        byte[] compressed = gzip(csv);
        int cut = compressed.length / 2;

        HttpClient client = mock(HttpClient.class);
        stubSends(
            client,
            List.of(
                // The body ends cleanly short of the declared range: an idle-dropped transfer.
                rangeResponse(compressed, 0, compressed.length, new ByteArrayInputStream(compressed, 0, cut)),
                rangeResponse(compressed, cut, compressed.length, new ByteArrayInputStream(compressed, cut, compressed.length - cut))
            )
        );

        StorageObject split = gzipSplit(client, compressed.length);
        try (InputStream in = split.newStream()) {
            assertArrayEquals("the resumed compressed body must decompress to the whole split", csv, in.readAllBytes());
        }
        assertEquals("one open plus one resume", 2, sentIfMatchHeaders.size());
        assertEquals("the resume carries the pin", ETAG, sentIfMatchHeaders.get(1));
    }

    /**
     * A genuinely truncated {@code .csv.gz}: the listing span matches the bytes the store holds, so there
     * is nothing to resume and the inflater hits the end of the DEFLATE stream. Since the complete
     * stored body was delivered, this is malformed compressed input (400), not a transient transport
     * truncation. A transport-short body is detected and resumed below the decompressor instead.
     */
    public void testTruncatedGzipObjectSurfacesAsClientError() throws Exception {
        byte[] compressed = gzip(csvPayload());
        byte[] truncated = Arrays.copyOf(compressed, compressed.length / 2);

        HttpClient client = mock(HttpClient.class);
        stubSends(client, List.of(rangeResponse(truncated, 0, truncated.length, new ByteArrayInputStream(truncated))));

        StorageObject split = gzipSplit(client, truncated.length);
        InputStream in = split.newStream();
        try {
            EOFException eof = expectThrows(EOFException.class, in::readAllBytes);
            assertThat(eof.getMessage(), Matchers.containsString("Unexpected end of ZLIB input stream"));
            RuntimeException classified = ExternalFailures.classify(eof);
            assertThat(classified, Matchers.instanceOf(ExternalClientException.class));
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
        } finally {
            split.abortStream(in);
        }
        assertEquals("a complete body over the split span must not be resumed", 1, sentIfMatchHeaders.size());
    }

    /** The stack a compressed whole-file split is read through, outermost first. */
    private static StorageObject gzipSplit(HttpClient client, long listingLength) {
        StorageObject provider = new HttpStorageObject(client, PATH, HttpConfiguration.defaults());
        StorageObject retryable = new RetryableStorageObject(provider, new RetryPolicy(3, 1, 10));
        return new DecompressingStorageObject(new RangeStorageObject(retryable, 0, listingLength), new GzipDecompressionCodec());
    }

    /** {@code If-Match} of every request the mocked client saw, in order. */
    private final List<String> sentIfMatchHeaders = new ArrayList<>();

    /**
     * Replays {@code responses} in order for successive {@code send} calls, recording each request's
     * {@code If-Match}. A Mockito answer rather than {@code thenReturn} chaining so the recording and the
     * ordering stay in one place.
     */
    private void stubSends(HttpClient client, List<HttpResponse<InputStream>> responses) throws Exception {
        when(client.send(any(), any())).thenAnswer(invocation -> {
            HttpRequest request = invocation.getArgument(0);
            sentIfMatchHeaders.add(request.headers().firstValue("If-Match").orElse(null));
            int n = sentIfMatchHeaders.size() - 1;
            assertThat("unexpected extra request", n, Matchers.lessThan(responses.size()));
            return responses.get(n);
        });
    }

    /**
     * A 206 whose {@code Content-Range} reports {@code total} as the object size, so the provider caches
     * that size and pins the returned ETag, exactly as a real range GET does.
     */
    private static HttpResponse<InputStream> partial(long from, long to, long total, InputStream body) {
        return response(
            PARTIAL_CONTENT,
            Map.of(
                "Content-Length",
                List.of(Long.toString(to - from)),
                "Content-Range",
                List.of("bytes " + from + "-" + (to - 1) + "/" + total),
                "ETag",
                List.of(ETAG)
            ),
            body
        );
    }

    private static HttpResponse<InputStream> rangeResponse(byte[] object, int from, int to, InputStream body) {
        return partial(from, to, object.length, body);
    }

    private static HttpResponse<InputStream> statusResponse(int status) {
        return response(status, Map.of(), new ByteArrayInputStream(new byte[0]));
    }

    @SuppressWarnings("unchecked")
    private static HttpResponse<InputStream> response(int status, Map<String, List<String>> headers, InputStream body) {
        HttpResponse<InputStream> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(status);
        when(response.headers()).thenReturn(HttpHeaders.of(headers, (a, b) -> true));
        when(response.body()).thenReturn(body);
        return response;
    }

    private static byte[] csvPayload() {
        StringBuilder csv = new StringBuilder("id,name,value\n");
        for (int i = 0; i < 5_000; i++) {
            csv.append(i).append(",name_").append(i).append(',').append(i * 3).append('\n');
        }
        return csv.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] gzip(byte[] input) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(input);
        }
        return out.toByteArray();
    }

    /** Delivers the first {@code prefix} bytes of {@code data}, then throws {@code fault}. */
    private static final class PrefixThenFaultStream extends InputStream {
        private final byte[] data;
        private final int prefix;
        private final IOException fault;
        private int pos = 0;

        PrefixThenFaultStream(byte[] data, int prefix, IOException fault) {
            this.data = data;
            this.prefix = prefix;
            this.fault = fault;
        }

        @Override
        public int read() throws IOException {
            if (pos >= prefix) {
                throw fault;
            }
            return data[pos++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (pos >= prefix) {
                throw fault;
            }
            int n = Math.min(len, prefix - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
        }
    }
}
