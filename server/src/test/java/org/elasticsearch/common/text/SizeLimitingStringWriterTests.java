/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.text;

import org.elasticsearch.test.ESTestCase;

public class SizeLimitingStringWriterTests extends ESTestCase {
    public void testSizeIsLimited() {
        SizeLimitingStringWriter writer = new SizeLimitingStringWriter(10);

        writer.write("a".repeat(10));

        // test all the methods
        expectThrows(SizeLimitingStringWriter.SizeLimitExceededException.class, () -> writer.write('a'));
        expectThrows(SizeLimitingStringWriter.SizeLimitExceededException.class, () -> writer.write("a"));
        expectThrows(SizeLimitingStringWriter.SizeLimitExceededException.class, () -> writer.write(new char[1]));
        expectThrows(SizeLimitingStringWriter.SizeLimitExceededException.class, () -> writer.write(new char[1], 0, 1));
        expectThrows(SizeLimitingStringWriter.SizeLimitExceededException.class, () -> writer.append('a'));
        expectThrows(SizeLimitingStringWriter.SizeLimitExceededException.class, () -> writer.append("a"));
        expectThrows(SizeLimitingStringWriter.SizeLimitExceededException.class, () -> writer.append("a", 0, 1));
    }
}
