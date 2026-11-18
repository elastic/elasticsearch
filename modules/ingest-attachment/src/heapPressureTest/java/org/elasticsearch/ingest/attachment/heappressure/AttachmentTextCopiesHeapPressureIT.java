/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.rest.ObjectPath;

import static org.hamcrest.Matchers.equalTo;

/**
 * A document whose extracted text fits on the heap must be indexed, not run the node out of memory.
 *
 * <p>The spreadsheet extracts to 30M chars, 60 MB of text with two bytes per char, a quarter of the 256 MB heap. The processor keeps
 * several full copies of the text at once: the growing output buffer of Tika and its {@code toString()}, the {@code trim()} of that, and
 * the char array {@code LanguageIdentifier} copies the text into, besides the base64 source and the decoded bytes. Today that exhausts
 * the heap while {@code LanguageIdentifier} copies the text, the same frame as in the incident: 25M chars still passed, 28M did not.
 * Once the processor keeps no more copies than it needs, the document is indexed.
 */
public class AttachmentTextCopiesHeapPressureIT extends AttachmentHeapPressureTestCase {

    private static final long TEXT_CHARS = 30_000_000;

    public void testTextFittingOnHeapIsIndexed() throws Exception {
        assertSmallSpreadsheetIndexed("small");

        Spreadsheet large = Spreadsheet.withTextChars(TEXT_CHARS);
        assertNull(errorBodyOrNullUnlessNodeDied(indexRequest("large", large)));
        // An out-of-memory node would not answer.
        assertNodeAlive();

        Request count = new Request("GET", "/" + INDEX + "/_count");
        assertThat(ObjectPath.createFromResponse(client().performRequest(count)).evaluate("count"), equalTo(2));

        // All of the text must have been extracted, so that truncating it can't pass for using less memory. This checks the length the
        // processor records rather than fetching the document, which would load its source onto the small heap.
        Request fullLength = new Request("GET", "/" + INDEX + "/_count");
        fullLength.setJsonEntity(Strings.format("""
            {"query": {"bool": {"filter": [
              {"ids": {"values": ["large"]}},
              {"range": {"attachment.content_length": {"gte": %d}}}
            ]}}}""", large.cellChars()));
        assertThat(ObjectPath.createFromResponse(client().performRequest(fullLength)).evaluate("count"), equalTo(1));
    }
}
