/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.common.http;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;

public class SizeLimitInputStreamTests extends ESTestCase {

    public void testGoodCase() throws IOException {
        int length = scaledRandomIntBetween(1, 100);
        test(length, length);
    }

    public void testLimitReached() {
        int length = scaledRandomIntBetween(1, 100);
        IOException e = expectThrows(IOException.class, () -> test(length+1, length));
        assertThat(e.getMessage(), is("Maximum limit of [" + length + "] bytes reached"));
    }

    public void testMarking() {
        ByteSizeValue byteSizeValue = new ByteSizeValue(1, ByteSizeUnit.BYTES);
        SizeLimitInputStream is = new SizeLimitInputStream(byteSizeValue,
                new ByteArrayInputStream("empty".getBytes(UTF_8)));
        assertThat(is.markSupported(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> is.mark(10));
        IOException e = expectThrows(IOException.class, () -> is.reset());
        assertThat(e.getMessage(), is("reset not supported"));
    }

    private void test(int inputStreamLength, int maxAllowedSize) throws IOException {
        String data = randomAlphaOfLength(inputStreamLength);
        ByteSizeValue byteSizeValue = new ByteSizeValue(maxAllowedSize, ByteSizeUnit.BYTES);
        SizeLimitInputStream is = new SizeLimitInputStream(byteSizeValue,
                new ByteArrayInputStream(data.getBytes(UTF_8)));

        if (randomBoolean()) {
            is.read(new byte[inputStreamLength]);
        } else {
            for (int i = 0; i < inputStreamLength; i++) {
                is.read();
            }
        }
    }
}
