package org.elasticsearch.ingest.attachment;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.apache.lucene.util.LuceneTestCase.SuppressFileSystems;
import org.apache.lucene.util.TestUtil;
import org.apache.tika.metadata.Metadata;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

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
            String parsedContent = TikaImpl.parse(bytes, new Metadata(), -1);
            assertNotNull(parsedContent);
            assertFalse(parsedContent.isEmpty());
            logger.debug("extracted content: {}", parsedContent);
        } catch (Exception e) {
            throw new RuntimeException("parsing of filename: " + fileName.getFileName() + " failed", e);
        }
    }
}
