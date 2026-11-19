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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.HeapPressureTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Base class for the {@code attachment} processor heap pressure scenarios. A single document goes through the pipeline Kibana File
 * Upload creates for every uploaded file, {@code attachment} with {@code indexed_chars: -1}, so all text Tika extracts is kept.
 *
 * <p>The documents are spreadsheets generated on the fly. Their repetitive cells compress well, so a request of a few MB expands to tens
 * or hundreds of millions of chars of extracted text, like the zipped Office file that ran a 1 GB heap serverless indexing node out of
 * memory. A single char outside Latin1 makes Java store the extracted text with two bytes per char, as in that incident.
 *
 * <p>Each scenario is its own class, because a node that died cannot be reused by another test and the source set gives every class its
 * own cluster.
 */
public abstract class AttachmentHeapPressureTestCase extends HeapPressureTestCase {

    protected static final String PIPELINE = "attachment";
    protected static final String INDEX = "attachments";

    /**
     * The scenarios are calibrated against this heap, scaled down from the 1 GB of the smallest serverless indexing nodes to keep the
     * documents small.
     */
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .module("ingest-attachment")
        .jvmArg("-Xms256m")
        .jvmArg("-Xmx256m")
        // An OutOfMemoryError must end the node so the test sees it.
        .jvmArg("-XX:+ExitOnOutOfMemoryError")
        .jvmArg("-XX:-HeapDumpOnOutOfMemoryError")
        // Only charged bytes count towards the parent breaker. Real memory accounting would also trip on uncharged memory, which hides
        // the accounting gaps these tests look for.
        .setting("indices.breaker.total.use_real_memory", "false")
        // Indexing pressure rejects a document that is larger than 10% of the heap, or that its pipeline grows by more than that, once
        // the pipeline completes. That bounds what the document retains afterwards, not what the processor needs while it runs, so it is
        // raised here to let the documents reach the processor and complete their pipeline.
        .setting("indexing_pressure.memory.limit", "90%")
        .setting("indexing_pressure.memory.max_operation_size", "90%")
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Override
    protected Settings restClientSettings() {
        // Extracting the text of the large documents takes a while; a dropped connection, not a timeout, is the death signal.
        return Settings.builder().put(super.restClientSettings()).put(CLIENT_SOCKET_TIMEOUT, "10m").build();
    }

    @Before
    public void createPipelineAndIndex() throws IOException {
        Request pipeline = new Request("PUT", "/_ingest/pipeline/" + PIPELINE);
        pipeline.setJsonEntity("""
            {
              "processors": [
                { "attachment": { "field": "data", "remove_binary": true, "indexed_chars": -1 } }
              ]
            }""");
        assertOK(client().performRequest(pipeline));

        Request index = new Request("PUT", "/" + INDEX);
        index.setJsonEntity("""
            {
              "settings": {"number_of_shards": 1, "number_of_replicas": 0},
              "mappings": {"properties": {"attachment": {"properties": {"content": {"type": "text"}}}}}
            }""");
        assertOK(client().performRequest(index));
    }

    /** A charge the processor leaks would make every later request more likely to trip. */
    @After
    public void assertRequestBreakerReleased() throws Exception {
        if (nodesAreAlive()) {
            assertBusy(() -> assertThat(requestBreakerUsedBytes(), equalTo(0L)));
        }
    }

    /**
     * Indexes a small spreadsheet and checks its text is extracted as expected: the large ones are generated the same way, so they
     * measure what they claim to, and the node still processes attachments.
     */
    protected void assertSmallSpreadsheetIndexed(String id) throws IOException {
        Spreadsheet small = new Spreadsheet(10, 4);
        assertNull(errorBodyOrNullUnlessNodeDied(indexRequest(id, small)));

        ObjectPath doc = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/" + INDEX + "/_doc/" + id)));
        String content = doc.evaluate("_source.attachment.content");
        assertThat(content, containsString(Spreadsheet.CELL_TEXT));
        assertThat(content.length(), greaterThanOrEqualTo(small.cellChars()));
        assertNull("the binary must have been removed", doc.evaluate("_source.data"));
    }

    /** A request that indexes the spreadsheet through the pipeline under test. */
    protected Request indexRequest(String id, Spreadsheet spreadsheet) throws IOException {
        byte[] xlsx = spreadsheet.toXlsx();
        logger.info("indexing spreadsheet [{}] of [{}] bytes with [{}] chars of cell text", id, xlsx.length, spreadsheet.cellChars());
        Request index = new Request("PUT", "/" + INDEX + "/_doc/" + id);
        index.addParameter("pipeline", PIPELINE);
        index.addParameter("refresh", "true");
        index.setJsonEntity(Strings.format("{\"data\":\"%s\"}", Base64.getEncoder().encodeToString(xlsx)));
        return index;
    }

    /**
     * Like {@link #errorBodyOrNull}, but a request that gets no response at all fails the test on the node having exited, if it did,
     * rather than on the closed connection.
     */
    protected Map<String, Object> errorBodyOrNullUnlessNodeDied(Request request) throws IOException {
        try {
            return errorBodyOrNull(request);
        } catch (IOException e) {
            // The connection closes while the process is still exiting, so give it a moment to go away before checking.
            if (waitUntil(() -> nodesAreAlive() == false, 30, TimeUnit.SECONDS)) {
                throw new AssertionError("the node exited while processing the request, most likely on an OutOfMemoryError", e);
            }
            throw e;
        }
    }

    /** The response body of a request that is expected to fail, logged so a failure shows what the node answered. */
    protected Map<String, Object> expectFailure(Request request) throws IOException {
        Map<String, Object> body = errorBodyOrNullUnlessNodeDied(request);
        assertNotNull("expected the request to fail but it succeeded", body);
        logger.info("request failed with: {}", body);
        return body;
    }

    /**
     * A minimal spreadsheet ({@code .xlsx}) with a single sheet whose cells all hold the same inline string. Written directly as OOXML,
     * so the test needs neither POI nor temporary files.
     */
    protected record Spreadsheet(int rows, int columns) {

        /** Ends with a char outside Latin1, so the extracted text is stored with two bytes per char. */
        static final String CELL_TEXT = "lorem ipsum dolor sit amet 0123456789 €";

        private static final int COLUMNS = 10;

        /** A spreadsheet with about {@code textChars} chars of extracted text: the cell text, and the separator Tika adds after each. */
        static Spreadsheet withTextChars(long textChars) {
            return new Spreadsheet(Math.toIntExact(textChars / ((long) COLUMNS * (CELL_TEXT.length() + 1))), COLUMNS);
        }

        /** Number of chars of cell text, a lower bound of the extracted text, which also holds separators and the sheet name. */
        int cellChars() {
            return Math.multiplyExact(Math.multiplyExact(rows, columns), CELL_TEXT.length());
        }

        byte[] toXlsx() throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
                addEntry(zip, "[Content_Types].xml", """
                    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                    <Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
                    <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
                    <Default Extension="xml" ContentType="application/xml"/>
                    <Override PartName="/xl/workbook.xml" \
                    ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
                    <Override PartName="/xl/worksheets/sheet1.xml" \
                    ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
                    </Types>""");
                addEntry(zip, "_rels/.rels", """
                    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                    <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
                    <Relationship Id="rId1" \
                    Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" \
                    Target="xl/workbook.xml"/>
                    </Relationships>""");
                addEntry(zip, "xl/workbook.xml", """
                    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                    <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" \
                    xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
                    <sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>
                    </workbook>""");
                addEntry(zip, "xl/_rels/workbook.xml.rels", """
                    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                    <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
                    <Relationship Id="rId1" \
                    Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" \
                    Target="worksheets/sheet1.xml"/>
                    </Relationships>""");

                zip.putNextEntry(new ZipEntry("xl/worksheets/sheet1.xml"));
                // not closed, that would close the zip stream
                Writer sheet = new OutputStreamWriter(zip, StandardCharsets.UTF_8);
                sheet.write("""
                    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                    <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>""");
                for (int row = 1; row <= rows; row++) {
                    sheet.write("<row r=\"" + row + "\">");
                    for (int column = 0; column < columns; column++) {
                        sheet.write("<c r=\"");
                        sheet.write(columnName(column));
                        sheet.write(Integer.toString(row));
                        sheet.write("\" t=\"inlineStr\"><is><t>");
                        sheet.write(CELL_TEXT);
                        sheet.write("</t></is></c>");
                    }
                    sheet.write("</row>");
                }
                sheet.write("</sheetData></worksheet>");
                sheet.flush();
                zip.closeEntry();
            }
            return bytes.toByteArray();
        }

        private static void addEntry(ZipOutputStream zip, String name, String content) throws IOException {
            zip.putNextEntry(new ZipEntry(name));
            zip.write(content.getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }

        private static String columnName(int column) {
            assert column < 26 : "only single letter columns supported";
            return String.valueOf((char) ('A' + column));
        }
    }
}
