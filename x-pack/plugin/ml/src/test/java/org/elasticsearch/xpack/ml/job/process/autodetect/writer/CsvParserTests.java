/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CsvParserTests extends ESTestCase {

    /**
     * Test parsing CSV with the NUL character code point (\0 or \u0000)
     */
    public void test() throws IOException {
        String data = "1422936876.262044869, 1422936876.262044869, 90, 2, 10.132.0.1, 0, 224.0.0.5, 0, 1, 1, 268435460, null, null, "
                + "null, null, null, null, null, null, null, null, null\n"
                + "1422943772.875342698, 1422943772.875342698, 90, 2, 10.132.0.1, 0, 224.0.0.5, 0, 1, 1, 268435460,,,,,\0,\u0000,,,,,"
                + "\u0000\n"
                + "\0";
        InputStream inputStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));

        CsvPreference csvPref = new CsvPreference.Builder(
                '"',
                ',',
                new String(new char[]{DataDescription.LINE_ENDING})).build();

        try (CsvListReader csvReader = new CsvListReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8),
                csvPref)) {
            String[] header = csvReader.getHeader(true);
            assertEquals(22, header.length);

            List<String> line = csvReader.read();
            assertEquals(22, line.size());

            // last line is \0
            line = csvReader.read();
            assertEquals(1, line.size());
        }
    }
}
