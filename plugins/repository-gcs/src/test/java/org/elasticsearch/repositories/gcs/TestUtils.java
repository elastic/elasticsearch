/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

final class TestUtils {

    private TestUtils() {}

    /**
     * Creates a random Service Account file for testing purpose
     */
    static byte[] createServiceAccount(final Random random) {
        try {
            final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
            keyPairGenerator.initialize(1024);
            final String privateKey = Base64.getEncoder().encodeToString(keyPairGenerator.generateKeyPair().getPrivate().getEncoded());

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), out)) {
                builder.startObject();
                {
                    builder.field("type", "service_account");
                    builder.field("project_id", "test");
                    builder.field("private_key_id", UUID.randomUUID().toString());
                    builder.field("private_key", "-----BEGIN PRIVATE KEY-----\n" + privateKey + "\n-----END PRIVATE KEY-----\n");
                    builder.field("client_email", "elastic@appspot.gserviceaccount.com");
                    builder.field("client_id", String.valueOf(Math.abs(random.nextLong())));
                }
                builder.endObject();
            }
            return out.toByteArray();
        } catch (Exception e) {
            throw new AssertionError("Unable to create service account file", e);
        }
    }

    static Optional<Tuple<String, BytesArray>> parseMultipartRequestBody(final InputStream requestBody) throws IOException {
        Tuple<String, BytesArray> content = null;
        try (BufferedInputStream in = new BufferedInputStream(new GZIPInputStream(requestBody))) {
            String name = null;
            int read;
            while ((read = in.read()) != -1) {
                boolean markAndContinue = false;
                try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                    do { // search next consecutive {carriage return, new line} chars and stop
                        if ((char) read == '\r') {
                            int next = in.read();
                            if (next != -1) {
                                if (next == '\n') {
                                    break;
                                }
                                out.write(read);
                                out.write(next);
                                continue;
                            }
                        }
                        out.write(read);
                    } while ((read = in.read()) != -1);

                    final String line = new String(out.toByteArray(), UTF_8);
                    if (line.length() == 0 || line.equals("\r\n") || line.startsWith("--")
                        || line.toLowerCase(Locale.ROOT).startsWith("content")) {
                        markAndContinue = true;
                    } else if (line.startsWith("{\"bucket\":\"bucket\"")) {
                        markAndContinue = true;
                        Matcher matcher = Pattern.compile("\"name\":\"([^\"]*)\"").matcher(line);
                        if (matcher.find()) {
                            name = matcher.group(1);
                        }
                    }
                    if (markAndContinue) {
                        in.mark(Integer.MAX_VALUE);
                        continue;
                    }
                }
                if (name != null) {
                    in.reset();
                    try (ByteArrayOutputStream binary = new ByteArrayOutputStream()) {
                        while ((read = in.read()) != -1) {
                            binary.write(read);
                        }
                        binary.flush();
                        byte[] tmp = binary.toByteArray();
                        // removes the trailing end "\r\n--__END_OF_PART__--\r\n" which is 23 bytes long
                        content = Tuple.tuple(name, new BytesArray(Arrays.copyOf(tmp, tmp.length - 23)));
                    }
                }
            }
        }
        return Optional.ofNullable(content);
    }

    private static final Pattern PATTERN_CONTENT_RANGE = Pattern.compile("bytes ([^/]*)/([0-9\\*]*)");
    private static final Pattern PATTERN_CONTENT_RANGE_BYTES = Pattern.compile("([0-9]*)-([0-9]*)");

    private static Integer parse(final Pattern pattern, final String contentRange, final BiFunction<String, String, Integer> fn) {
        final Matcher matcher = pattern.matcher(contentRange);
        if (matcher.matches() == false || matcher.groupCount() != 2) {
            throw new IllegalArgumentException("Unable to parse content range header");
        }
        return fn.apply(matcher.group(1), matcher.group(2));
    }

    static Integer getContentRangeLimit(final String contentRange) {
        return parse(PATTERN_CONTENT_RANGE, contentRange, (bytes, limit) -> "*".equals(limit) ? null : Integer.parseInt(limit));
    }

    static int getContentRangeStart(final String contentRange) {
        return parse(PATTERN_CONTENT_RANGE, contentRange,
            (bytes, limit) -> parse(PATTERN_CONTENT_RANGE_BYTES, bytes,
                (start, end) -> Integer.parseInt(start)));
    }

    static int getContentRangeEnd(final String contentRange) {
        return parse(PATTERN_CONTENT_RANGE, contentRange,
            (bytes, limit) -> parse(PATTERN_CONTENT_RANGE_BYTES, bytes,
                (start, end) -> Integer.parseInt(end)));
    }
}
