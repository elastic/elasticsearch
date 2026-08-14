/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.resourceexhaustion;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that Elasticsearch returns 429 when fielddata loading exhausts available heap rather
 * than crashing the node with an OOM error.
 *
 * The cluster runs with a 512 MB heap. The default fielddata circuit breaker limit is 40% of
 * heap (~204 MB). This test indexes 8,000 documents each with a 30 KB unique text value,
 * producing ~240 MB of unique terms. A {@code terms} aggregation on that field triggers global
 * ordinal construction, which must load all unique terms into fielddata simultaneously. Without
 * the circuit breaker this would exhaust heap; with it the node returns 429.
 *
 * The field uses {@code "analyzer": "keyword"} so each full field value is a single term.
 * The default analyzer would tokenize the value into many short tokens, making fielddata
 * far smaller than the field byte size and preventing the breaker from firing.
 *
 * Documents are streamed from the test JVM during setup so the test process never holds
 * the full 240 MB payload in memory.
 */
public class FieldDataBreakerIT extends ResourceExhaustionSingleNodeTestCase {

    private static final String INDEX = "field-data-breaker";
    private static final int DOC_COUNT = 8_000;
    // 30 KB per doc, safely under Lucene's 32,766-byte max term size.
    // 8,000 × 30 KB = ~240 MB of unique terms, above the ~204 MB fielddata limit.
    private static final int TERM_BYTES = 30 * 1024;

    // ESRestTestCase initializes the client in @Before (per-test), not @BeforeClass, so the
    // client is null during @BeforeClass. Use a guarded @Before to run setup exactly once.
    private static final AtomicBoolean INDEX_CREATED = new AtomicBoolean(false);

    @Before
    public void ensureIndex() throws IOException {
        if (INDEX_CREATED.compareAndSet(false, true)) {
            Request create = new Request("PUT", "/" + INDEX);
            create.setJsonEntity("""
                {
                  "settings": { "number_of_replicas": 0 },
                  "mappings": {
                    "properties": {
                      "content": {
                        "type": "text",
                        "fielddata": true,
                        "analyzer": "keyword"
                      }
                    }
                  }
                }
                """);
            client().performRequest(create);

            Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
            bulk.setEntity(
                new InputStreamEntity(new BulkDocStream(INDEX, DOC_COUNT, TERM_BYTES), ContentType.create("application/x-ndjson"))
            );
            client().performRequest(bulk);

            client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
        }
    }

    public void testFieldDataBreakerTrips429() throws IOException {
        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.setJsonEntity("""
            {
              "size": 0,
              "aggs": { "all_terms": { "terms": { "field": "content", "size": 10000 } } }
            }
            """);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(search));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(429));
    }

    /**
     * Generates valid bulk ndjson for {@code docCount} documents, each with a {@code content}
     * field of {@code termBytes} bytes. The first 8 bytes are a zero-padded document number so
     * every term is unique; the remainder is padding. Streamed one buffer at a time so the test
     * JVM holds only a small window in memory at any point.
     */
    private static class BulkDocStream extends InputStream {

        private static final byte[] ACTION_PREFIX;
        private static final byte[] ACTION_SUFFIX = ("\"}}\\n").getBytes(StandardCharsets.UTF_8);
        private static final byte[] DOC_PREFIX = ("{\"content\":\"").getBytes(StandardCharsets.UTF_8);
        private static final byte[] DOC_SUFFIX = ("\"}\n").getBytes(StandardCharsets.UTF_8);
        private static final byte PADDING_BYTE = 'a';

        static {
            ACTION_PREFIX = ("{\"index\":{\"_index\":\"").getBytes(StandardCharsets.UTF_8);
        }

        private final byte[] actionLine;
        private final int docCount;
        private final int termBytes;

        private int currentDoc = 0;

        // Which section of the current doc we are streaming
        private enum Section {
            ACTION,
            DOC_PREFIX,
            DOC_ID,
            PADDING,
            DOC_SUFFIX
        }

        private Section section = Section.ACTION;
        private int sectionOffset = 0;
        private byte[] docIdBytes;
        private int paddingRemaining;

        BulkDocStream(String index, int docCount, int termBytes) {
            this.actionLine = ("{\"index\":{\"_index\":\"" + index + "\"}}\n").getBytes(StandardCharsets.UTF_8);
            this.docCount = docCount;
            this.termBytes = termBytes;
            startDoc();
        }

        private void startDoc() {
            docIdBytes = String.format("%08d", currentDoc).getBytes(StandardCharsets.UTF_8);
            paddingRemaining = termBytes - docIdBytes.length;
            section = Section.ACTION;
            sectionOffset = 0;
        }

        @Override
        public int read() throws IOException {
            byte[] buf = new byte[1];
            int n = read(buf, 0, 1);
            return n == -1 ? -1 : (buf[0] & 0xff);
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (currentDoc >= docCount) {
                return -1;
            }
            int written = 0;
            while (written < len && currentDoc < docCount) {
                switch (section) {
                    case ACTION -> {
                        int chunk = Math.min(len - written, actionLine.length - sectionOffset);
                        System.arraycopy(actionLine, sectionOffset, buf, off + written, chunk);
                        written += chunk;
                        sectionOffset += chunk;
                        if (sectionOffset >= actionLine.length) {
                            section = Section.DOC_PREFIX;
                            sectionOffset = 0;
                        }
                    }
                    case DOC_PREFIX -> {
                        int chunk = Math.min(len - written, DOC_PREFIX.length - sectionOffset);
                        System.arraycopy(DOC_PREFIX, sectionOffset, buf, off + written, chunk);
                        written += chunk;
                        sectionOffset += chunk;
                        if (sectionOffset >= DOC_PREFIX.length) {
                            section = Section.DOC_ID;
                            sectionOffset = 0;
                        }
                    }
                    case DOC_ID -> {
                        int chunk = Math.min(len - written, docIdBytes.length - sectionOffset);
                        System.arraycopy(docIdBytes, sectionOffset, buf, off + written, chunk);
                        written += chunk;
                        sectionOffset += chunk;
                        if (sectionOffset >= docIdBytes.length) {
                            section = Section.PADDING;
                        }
                    }
                    case PADDING -> {
                        int chunk = Math.min(len - written, paddingRemaining);
                        java.util.Arrays.fill(buf, off + written, off + written + chunk, PADDING_BYTE);
                        written += chunk;
                        paddingRemaining -= chunk;
                        if (paddingRemaining <= 0) {
                            section = Section.DOC_SUFFIX;
                            sectionOffset = 0;
                        }
                    }
                    case DOC_SUFFIX -> {
                        int chunk = Math.min(len - written, DOC_SUFFIX.length - sectionOffset);
                        System.arraycopy(DOC_SUFFIX, sectionOffset, buf, off + written, chunk);
                        written += chunk;
                        sectionOffset += chunk;
                        if (sectionOffset >= DOC_SUFFIX.length) {
                            currentDoc++;
                            if (currentDoc < docCount) {
                                startDoc();
                            }
                        }
                    }
                }
            }
            return written == 0 ? -1 : written;
        }
    }
}
