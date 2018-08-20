/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructurefinder;

import com.ibm.icu.text.CharsetMatch;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class LogStructureFinderManagerTests extends LogStructureTestCase {

    private LogStructureFinderManager structureFinderManager = new LogStructureFinderManager();

    public void testFindCharsetGivenCharacterWidths() throws Exception {

        for (Charset charset : Arrays.asList(StandardCharsets.UTF_8, StandardCharsets.UTF_16LE, StandardCharsets.UTF_16BE)) {
            CharsetMatch charsetMatch = structureFinderManager.findCharset(explanation,
                new ByteArrayInputStream(TEXT_SAMPLE.getBytes(charset)));
            assertEquals(charset.name(), charsetMatch.getName());
        }
    }

    public void testFindCharsetGivenBinary() throws Exception {

        // This input should never match a single byte character set.  ICU4J will sometimes decide
        // that it matches a double byte character set, hence the two assertion branches.
        int size = 1000;
        byte[] binaryBytes = randomByteArrayOfLength(size);
        for (int i = 0; i < 10; ++i) {
            binaryBytes[randomIntBetween(0, size - 1)] = 0;
        }

        try {
            CharsetMatch charsetMatch = structureFinderManager.findCharset(explanation, new ByteArrayInputStream(binaryBytes));
            assertThat(charsetMatch.getName(), startsWith("UTF-16"));
        } catch (IllegalArgumentException e) {
            assertEquals("Could not determine a usable character encoding for the input - could it be binary data?", e.getMessage());
        }
    }

    public void testMakeBestStructureGivenJson() throws Exception {
        assertThat(structureFinderManager.makeBestStructureFinder(explanation,
            "{ \"time\": \"2018-05-17T13:41:23\", \"message\": \"hello\" }", StandardCharsets.UTF_8.name(), randomBoolean()),
            instanceOf(JsonLogStructureFinder.class));
    }

    public void testMakeBestStructureGivenXml() throws Exception {
        assertThat(structureFinderManager.makeBestStructureFinder(explanation,
            "<log time=\"2018-05-17T13:41:23\"><message>hello</message></log>", StandardCharsets.UTF_8.name(), randomBoolean()),
            instanceOf(XmlLogStructureFinder.class));
    }

    public void testMakeBestStructureGivenCsv() throws Exception {
        assertThat(structureFinderManager.makeBestStructureFinder(explanation, "time,message\n" +
                "2018-05-17T13:41:23,hello\n", StandardCharsets.UTF_8.name(), randomBoolean()),
            instanceOf(SeparatedValuesLogStructureFinder.class));
    }

    public void testMakeBestStructureGivenText() throws Exception {
        assertThat(structureFinderManager.makeBestStructureFinder(explanation, "[2018-05-17T13:41:23] hello\n" +
                "[2018-05-17T13:41:24] hello again\n", StandardCharsets.UTF_8.name(), randomBoolean()),
            instanceOf(TextLogStructureFinder.class));
    }
}
