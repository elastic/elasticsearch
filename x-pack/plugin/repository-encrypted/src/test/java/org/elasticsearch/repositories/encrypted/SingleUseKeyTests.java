/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SingleUseKeyTests extends ESTestCase {

    byte[] testKeyPlaintext;
    SecretKey testKey;
    BytesReference testKeyId;

    @Before
    public void setUpMocks() {
        testKeyPlaintext = randomByteArrayOfLength(32);
        testKey = new SecretKeySpec(testKeyPlaintext, "AES");
        testKeyId = new BytesArray(randomAlphaOfLengthBetween(2, 32));
    }

    public void testNewKeySupplier() throws Exception {
        CheckedSupplier<SingleUseKey, IOException> singleUseKeySupplier = SingleUseKey.createSingleUseKeySupplier(
            () -> new Tuple<>(testKeyId, testKey)
        );
        SingleUseKey generatedSingleUseKey = singleUseKeySupplier.get();
        assertThat(generatedSingleUseKey.getKeyId(), equalTo(testKeyId));
        assertThat(generatedSingleUseKey.getNonce(), equalTo(SingleUseKey.MIN_NONCE));
        assertThat(generatedSingleUseKey.getKey().getEncoded(), equalTo(testKeyPlaintext));
    }

    public void testNonceIncrement() throws Exception {
        int nonce = randomIntBetween(SingleUseKey.MIN_NONCE, SingleUseKey.MAX_NONCE - 2);
        SingleUseKey singleUseKey = new SingleUseKey(testKeyId, testKey, nonce);
        AtomicReference<SingleUseKey> keyCurrentlyInUse = new AtomicReference<>(singleUseKey);
        @SuppressWarnings("unchecked")
        CheckedSupplier<Tuple<BytesReference, SecretKey>, IOException> keyGenerator = mock(CheckedSupplier.class);
        CheckedSupplier<SingleUseKey, IOException> singleUseKeySupplier = SingleUseKey.internalSingleUseKeySupplier(
            keyGenerator,
            keyCurrentlyInUse
        );
        SingleUseKey generatedSingleUseKey = singleUseKeySupplier.get();
        assertThat(generatedSingleUseKey.getKeyId(), equalTo(testKeyId));
        assertThat(generatedSingleUseKey.getNonce(), equalTo(nonce));
        assertThat(generatedSingleUseKey.getKey().getEncoded(), equalTo(testKeyPlaintext));
        SingleUseKey generatedSingleUseKey2 = singleUseKeySupplier.get();
        assertThat(generatedSingleUseKey2.getKeyId(), equalTo(testKeyId));
        assertThat(generatedSingleUseKey2.getNonce(), equalTo(nonce + 1));
        assertThat(generatedSingleUseKey2.getKey().getEncoded(), equalTo(testKeyPlaintext));
        verifyNoMoreInteractions(keyGenerator);
    }

    public void testConcurrentWrapAround() throws Exception {
        int nThreads = 3;
        TestThreadPool testThreadPool = new TestThreadPool(
            "SingleUserKeyTests#testConcurrentWrapAround",
            Settings.builder()
                .put("thread_pool." + ThreadPool.Names.GENERIC + ".size", nThreads)
                .put("thread_pool." + ThreadPool.Names.GENERIC + ".queue_size", 1)
                .build()
        );
        int nonce = SingleUseKey.MAX_NONCE;
        SingleUseKey singleUseKey = new SingleUseKey(null, null, nonce);

        AtomicReference<SingleUseKey> keyCurrentlyInUse = new AtomicReference<>(singleUseKey);
        @SuppressWarnings("unchecked")
        CheckedSupplier<Tuple<BytesReference, SecretKey>, IOException> keyGenerator = mock(CheckedSupplier.class);
        when(keyGenerator.get()).thenReturn(new Tuple<>(testKeyId, testKey));
        CheckedSupplier<SingleUseKey, IOException> singleUseKeySupplier = SingleUseKey.internalSingleUseKeySupplier(
            keyGenerator,
            keyCurrentlyInUse
        );
        List<SingleUseKey> generatedKeys = new ArrayList<>(nThreads);
        for (int i = 0; i < nThreads; i++) {
            generatedKeys.add(null);
        }
        for (int i = 0; i < nThreads; i++) {
            final int resultIdx = i;
            testThreadPool.generic().execute(() -> {
                try {
                    generatedKeys.set(resultIdx, singleUseKeySupplier.get());
                } catch (IOException e) {
                    fail();
                }
            });
        }
        terminate(testThreadPool);
        verify(keyGenerator, times(1)).get();
        assertThat(keyCurrentlyInUse.get().getNonce(), equalTo(SingleUseKey.MIN_NONCE + nThreads));
        assertThat(generatedKeys.stream().map(suk -> suk.getKey()).collect(Collectors.toSet()).size(), equalTo(1));
        assertThat(
            generatedKeys.stream().map(suk -> suk.getKey().getEncoded()).collect(Collectors.toSet()).iterator().next(),
            equalTo(testKeyPlaintext)
        );
        assertThat(generatedKeys.stream().map(suk -> suk.getKeyId()).collect(Collectors.toSet()).iterator().next(), equalTo(testKeyId));
        assertThat(generatedKeys.stream().map(suk -> suk.getNonce()).collect(Collectors.toSet()).size(), equalTo(nThreads));
        assertThat(
            generatedKeys.stream().map(suk -> suk.getNonce()).collect(Collectors.toSet()),
            contains(SingleUseKey.MIN_NONCE, SingleUseKey.MIN_NONCE + 1, SingleUseKey.MIN_NONCE + 2)
        );
    }

    public void testNonceWrapAround() throws Exception {
        int nonce = SingleUseKey.MAX_NONCE;
        SingleUseKey singleUseKey = new SingleUseKey(testKeyId, testKey, nonce);
        AtomicReference<SingleUseKey> keyCurrentlyInUse = new AtomicReference<>(singleUseKey);
        byte[] newTestKeyPlaintext = randomByteArrayOfLength(32);
        SecretKey newTestKey = new SecretKeySpec(newTestKeyPlaintext, "AES");
        BytesReference newTestKeyId = new BytesArray(randomAlphaOfLengthBetween(2, 32));
        CheckedSupplier<SingleUseKey, IOException> singleUseKeySupplier = SingleUseKey.internalSingleUseKeySupplier(
            () -> new Tuple<>(newTestKeyId, newTestKey),
            keyCurrentlyInUse
        );
        SingleUseKey generatedSingleUseKey = singleUseKeySupplier.get();
        assertThat(generatedSingleUseKey.getKeyId(), equalTo(newTestKeyId));
        assertThat(generatedSingleUseKey.getNonce(), equalTo(SingleUseKey.MIN_NONCE));
        assertThat(generatedSingleUseKey.getKey().getEncoded(), equalTo(newTestKeyPlaintext));
    }

    public void testGeneratorException() {
        int nonce = SingleUseKey.MAX_NONCE;
        SingleUseKey singleUseKey = new SingleUseKey(null, null, nonce);
        AtomicReference<SingleUseKey> keyCurrentlyInUse = new AtomicReference<>(singleUseKey);
        CheckedSupplier<SingleUseKey, IOException> singleUseKeySupplier = SingleUseKey.internalSingleUseKeySupplier(
            () -> { throw new IOException("expected exception"); },
            keyCurrentlyInUse
        );
        expectThrows(IOException.class, () -> singleUseKeySupplier.get());
    }
}
