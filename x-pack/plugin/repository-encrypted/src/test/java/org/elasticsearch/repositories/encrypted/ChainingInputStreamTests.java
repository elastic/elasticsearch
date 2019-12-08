/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ChainingInputStreamTests extends ESTestCase {

    // test pass through element wise
    // test empty component input stream
    // test stream ends when receives null

    public void testEmpty() throws Exception {
        ChainingInputStream emptyStream = newEmptyStream();
        assertThat(emptyStream.read(), Matchers.is(-1));
        emptyStream = newEmptyStream();
        byte[] b = new byte[1 + Randomness.get().nextInt(8)];
        int off = Randomness.get().nextInt(b.length);
        assertThat(emptyStream.read(b, off, b.length - off), Matchers.is(-1));
        emptyStream = newEmptyStream();
        assertThat(emptyStream.available(), Matchers.is(0));
        emptyStream = newEmptyStream();
        assertThat(emptyStream.skip(1 + Randomness.get().nextInt(32)), Matchers.is(0L));
    }

    public void testHeadComponentIsNull() throws Exception {
        AtomicReference<InputStream> headInputStream = new AtomicReference<>();
        AtomicBoolean nextCalled = new AtomicBoolean(false);
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                headInputStream.set(currentComponentIn);
                nextCalled.set(true);
                return null;
            }
        };
        assertThat(test.read(), Matchers.is(-1));
        assertThat(nextCalled.get(), Matchers.is(true));
        assertThat(headInputStream.get(), Matchers.nullValue());
    }

    public void testReadAll() throws Exception {
        byte[] b = new byte[2 + Randomness.get().nextInt(32)];
        Randomness.get().nextBytes(b);
        int splitIdx = Randomness.get().nextInt(b.length - 1);
        ByteArrayInputStream first = new ByteArrayInputStream(b, 0, splitIdx + 1);
        ByteArrayInputStream second = new ByteArrayInputStream(b, splitIdx + 1, b.length - splitIdx - 1);
        ChainingInputStream test = new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentElementIn) throws IOException {
                if (currentElementIn == null) {
                    return first;
                } else if (currentElementIn == first) {
                    return second;
                } else if (currentElementIn == second) {
                    return null;
                } else {
                    throw new IllegalArgumentException();
                }
            }
        };
        byte[] result = test.readAllBytes();
        assertThat(result.length, Matchers.is(b.length));
        for (int i = 0; i < result.length; i++) {
            Assert.assertThat(result[i], Matchers.is(b[i]));
        }
    }

    private ChainingInputStream newEmptyStream() {
        return new ChainingInputStream() {
            @Override
            InputStream nextComponent(InputStream currentElementIn) throws IOException {
                return null;
            }
        };
    }

}
