/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.index.IndexFileNames;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.containsString;

public class LuceneFilesExtensionsTests extends ESTestCase {

    public void testUnknownFileExtension() {
        if (Assertions.ENABLED) {
            AssertionError e = expectThrows(AssertionError.class, () -> LuceneFilesExtensions.fromExtension("abc"));
            assertThat(e.getMessage(), containsString("unknown Lucene file extension [abc]"));
            assertFalse(LuceneFilesExtensions.isLuceneExtension("abc"));

            setEsAllowUnknownLuceneFileExtensions("true");
            try {
                assertNull(LuceneFilesExtensions.fromExtension("abc"));
            } finally {
                setEsAllowUnknownLuceneFileExtensions(null);
            }
        } else {
            assertNull(LuceneFilesExtensions.fromExtension("abc"));
        }
    }

    @SuppressForbidden(reason = "set or clear system property es.allow_unknown_lucene_file_extensions")
    public void setEsAllowUnknownLuceneFileExtensions(final String value) {
        if (value == null) {
            System.clearProperty("es.allow_unknown_lucene_file_extensions");
        } else {
            System.setProperty("es.allow_unknown_lucene_file_extensions", value);
        }
    }

    public void testIsLuceneExtension() {
        assertFalse(LuceneFilesExtensions.isLuceneExtension(null));
        assertFalse(LuceneFilesExtensions.isLuceneExtension("bcde"));
        String randomStringWithLuceneExtension = randomAlphanumericOfLength(10)
            + "."
            + LuceneFilesExtensions.values()[randomInt(LuceneFilesExtensions.values().length - 1)].getExtension();
        String extension = IndexFileNames.getExtension(randomStringWithLuceneExtension);
        assertTrue(extension + " should be considered a Lucene extension", LuceneFilesExtensions.isLuceneExtension(extension));
        String upperCaseExtension = extension.toUpperCase(Locale.ROOT);
        assertFalse(
            upperCaseExtension + " (uppercase) should not be considered a Lucene extension",
            LuceneFilesExtensions.isLuceneExtension(upperCaseExtension)
        );
    }
}
