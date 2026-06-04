/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.apache.commons.io.IOUtils;
import org.apache.tika.metadata.TikaCoreProperties;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that {@link LocalExtractionBackend} produces the same content and metadata as the
 * prior inline {@code TikaImpl.parse()} invocation in {@link AttachmentProcessor}.
 */
public class LocalExtractionBackendTests extends ESTestCase {

    private final LocalExtractionBackend backend = new LocalExtractionBackend();

    public void testExtractEnglishText() throws Exception {
        ExtractionResult result = extract("text-in-english.txt", null, -1);
        assertThat(result.content().trim(), is("\"God Save the Queen\" (alternatively \"God Save the King\""));
        assertThat(result.metadata().get("Content-Type"), containsString("text/plain"));
    }

    public void testExtractPdf() throws Exception {
        ExtractionResult result = extract("test.pdf", null, -1);
        assertThat(
            result.content().trim(),
            is("This is a test, with umlauts, from München\n\nAlso contains newlines for testing.\n\nAnd one more.")
        );
        assertThat(result.metadata().get("Content-Type"), is("application/pdf"));
    }

    public void testExtractWordDocument() throws Exception {
        ExtractionResult result = extract("issue-104.docx", null, -1);
        assertThat(result.content(), notNullValue());
        assertThat(result.metadata().get(TikaCoreProperties.CREATED.getName()), is("2012-10-12T11:17:00Z"));
        assertThat(result.metadata().get(TikaCoreProperties.CREATOR.getName()), is("Windows User"));
    }

    public void testIndexedCharsLimit() throws Exception {
        ExtractionResult result = extract("text-in-english.txt", null, 19);
        assertThat(result.content(), equalTo("\"God Save the Queen"));
    }

    public void testResourceNameHint() throws Exception {
        // CJK file needs filename hint for charset detection
        ExtractionResult result = extract("text-cjk-big5.txt", "text-cjk-big5.txt", -1);
        assertThat(result.content(), containsString("碩鼠碩鼠"));
        assertThat(result.metadata().get("Content-Type"), containsString("charset=Big5"));
    }

    public void testZeroByteInput() {
        // Zero-byte input should produce empty content without throwing
        ExtractionResult result = extractSync(new byte[0], null, -1);
        assertThat(result.content(), is(""));
    }

    public void testListenerFiredSynchronously() {
        // Confirm LocalExtractionBackend calls the listener before extract() returns
        boolean[] called = { false };
        backend.extract(new byte[1], null, -1, ActionListener.wrap(r -> called[0] = true, e -> called[0] = true));
        assertTrue("listener must be called synchronously", called[0]);
    }

    // ---------- helpers ----------

    private ExtractionResult extract(String file, String resourceName, int maxChars) throws Exception {
        String path = "/org/elasticsearch/ingest/attachment/test/sample-files/" + file;
        byte[] bytes;
        try (InputStream is = LocalExtractionBackendTests.class.getResourceAsStream(path)) {
            bytes = IOUtils.toByteArray(is);
        }
        return extractSync(bytes, resourceName, maxChars);
    }

    private ExtractionResult extractSync(byte[] content, String resourceName, int maxChars) {
        AtomicReference<ExtractionResult> resultRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        backend.extract(content, resourceName, maxChars, ActionListener.wrap(resultRef::set, exceptionRef::set));
        assertNull("extraction should not have failed: " + exceptionRef.get(), exceptionRef.get());
        assertNotNull("extraction result must not be null", resultRef.get());
        return resultRef.get();
    }
}
