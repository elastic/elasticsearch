/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.tika.Tika;
import org.apache.tika.langdetect.tika.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Evil test-coverage cheat, we parse a bunch of docs from tika
 * so that we have a nice grab-bag variety, and assert some content
 * comes back and no exception.
 */
@SuppressFileSystems("ExtrasFS") // don't try to parse extraN
public class TikaDocTests extends ESTestCase {

    /** some test files from tika test suite, zipped up */
    static final String TIKA_FILES = "/org/elasticsearch/ingest/attachment/test/tika-files/";

    public void testFiles() throws Exception {
        Path tmp = createTempDir();
        logger.debug("unzipping all tika sample files");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(PathUtils.get(getClass().getResource(TIKA_FILES).toURI()))) {
            for (Path doc : stream) {
                String filename = doc.getFileName().toString();
                TestUtil.unzip(getClass().getResourceAsStream(TIKA_FILES + filename), tmp);
            }
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tmp)) {
            for (Path doc : stream) {
                logger.debug("parsing: {}", doc);
                assertParseable(doc);
            }
        }
    }

    void assertParseable(Path fileName) throws Exception {
        try {
            byte bytes[] = Files.readAllBytes(fileName);
            ChunkedTextWriter writer = new ChunkedTextWriter(randomFrom(1, between(2, 100), ChunkedTextWriter.DEFAULT_CHUNK_SIZE));
            Metadata metadata = new Metadata();
            TikaImpl.parse(bytes, metadata, -1, writer);
            assertThat(writer.length(), greaterThan(0L));

            // text, length, language and metadata must match Tika's own parseToString
            Metadata expectedMetadata = new Metadata();
            String expected = new Tika(TikaImpl.PARSER_INSTANCE.getDetector(), TikaImpl.PARSER_INSTANCE).parseToString(
                new ByteArrayInputStream(bytes),
                expectedMetadata,
                -1
            );
            assertThat(writer.length(), equalTo((long) expected.length()));
            assertThat(AttachmentProcessor.detectLanguage(writer, -1), equalTo(new LanguageIdentifier(expected).getLanguage()));
            String parsedContent = writer.toTrimmedString();
            assertThat(parsedContent, equalTo(expected.trim()));
            assertThat(metadata.names(), arrayContainingInAnyOrder(expectedMetadata.names()));
            for (String name : expectedMetadata.names()) {
                // recorded embedded exceptions include stack traces, which differ between calls
                if (name.startsWith(TikaCoreProperties.TIKA_META_EXCEPTION_PREFIX) == false) {
                    assertThat(name, metadata.getValues(name), equalTo(expectedMetadata.getValues(name)));
                }
            }
            logger.debug("extracted content: {}", parsedContent);
        } catch (Exception e) {
            throw new RuntimeException("parsing of filename: " + fileName.getFileName() + " failed", e);
        }
    }
}
