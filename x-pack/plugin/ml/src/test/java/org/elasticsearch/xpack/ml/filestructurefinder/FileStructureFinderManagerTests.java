/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import com.ibm.icu.text.CharsetMatch;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.elasticsearch.xpack.ml.filestructurefinder.FileStructureOverrides.EMPTY_OVERRIDES;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class FileStructureFinderManagerTests extends FileStructureTestCase {

    private FileStructureFinderManager structureFinderManager = new FileStructureFinderManager();

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
        assertThat(structureFinderManager.makeBestStructureFinder(explanation, JSON_SAMPLE, StandardCharsets.UTF_8.name(), randomBoolean(),
            EMPTY_OVERRIDES), instanceOf(JsonFileStructureFinder.class));
    }

    public void testMakeBestStructureGivenJsonAndDelimitedOverride() throws Exception {

        // Need to change the quote character from the default of double quotes
        // otherwise the quotes in the JSON will stop it parsing as CSV
        FileStructureOverrides overrides = FileStructureOverrides.builder()
            .setFormat(FileStructure.Format.DELIMITED).setQuote('\'').build();

        assertThat(structureFinderManager.makeBestStructureFinder(explanation, JSON_SAMPLE, StandardCharsets.UTF_8.name(), randomBoolean(),
            overrides), instanceOf(DelimitedFileStructureFinder.class));
    }

    public void testMakeBestStructureGivenXml() throws Exception {
        assertThat(structureFinderManager.makeBestStructureFinder(explanation, XML_SAMPLE, StandardCharsets.UTF_8.name(), randomBoolean(),
            EMPTY_OVERRIDES), instanceOf(XmlFileStructureFinder.class));
    }

    public void testMakeBestStructureGivenXmlAndTextOverride() throws Exception {

        FileStructureOverrides overrides = FileStructureOverrides.builder().setFormat(FileStructure.Format.SEMI_STRUCTURED_TEXT).build();

        assertThat(structureFinderManager.makeBestStructureFinder(explanation, XML_SAMPLE, StandardCharsets.UTF_8.name(), randomBoolean(),
            overrides), instanceOf(TextLogFileStructureFinder.class));
    }

    public void testMakeBestStructureGivenCsv() throws Exception {
        assertThat(structureFinderManager.makeBestStructureFinder(explanation, CSV_SAMPLE, StandardCharsets.UTF_8.name(), randomBoolean(),
            EMPTY_OVERRIDES), instanceOf(DelimitedFileStructureFinder.class));
    }

    public void testMakeBestStructureGivenCsvAndJsonOverride() {

        FileStructureOverrides overrides = FileStructureOverrides.builder().setFormat(FileStructure.Format.JSON).build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> structureFinderManager.makeBestStructureFinder(explanation, CSV_SAMPLE, StandardCharsets.UTF_8.name(), randomBoolean(),
                overrides));

        assertEquals("Input did not match the specified format [json]", e.getMessage());
    }

    public void testMakeBestStructureGivenText() throws Exception {
        assertThat(structureFinderManager.makeBestStructureFinder(explanation, TEXT_SAMPLE, StandardCharsets.UTF_8.name(), randomBoolean(),
            EMPTY_OVERRIDES), instanceOf(TextLogFileStructureFinder.class));
    }

    public void testMakeBestStructureGivenTextAndDelimitedOverride() throws Exception {

        // Every line of the text sample has two colons, so colon delimited is possible, just very weird
        FileStructureOverrides overrides = FileStructureOverrides.builder()
            .setFormat(FileStructure.Format.DELIMITED).setDelimiter(':').build();

        assertThat(structureFinderManager.makeBestStructureFinder(explanation, TEXT_SAMPLE, StandardCharsets.UTF_8.name(), randomBoolean(),
            overrides), instanceOf(DelimitedFileStructureFinder.class));
    }
}
