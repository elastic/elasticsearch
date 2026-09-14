/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.patch;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Locale;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class FsDirectoryFactoryTransformerTests extends ESTestCase {

    public void testTransformIgnoresUnrelatedClasses() throws Exception {
        FsDirectoryFactoryTransformer transformer = new FsDirectoryFactoryTransformer();
        byte[] result = transformer.transform(
            getClass().getClassLoader(),
            "org/elasticsearch/index/store/SomeOtherClass",
            null,
            null,
            "irrelevant bytes".getBytes(StandardCharsets.UTF_8)
        );
        assertThat(result, nullValue());
    }

    public void testTransformIgnoresNullClassName() throws Exception {
        FsDirectoryFactoryTransformer transformer = new FsDirectoryFactoryTransformer();
        byte[] result = transformer.transform(
            getClass().getClassLoader(),
            null,
            null,
            null,
            "irrelevant bytes".getBytes(StandardCharsets.UTF_8)
        );
        assertThat(result, nullValue());
    }

    public void testTransformNoOpsForRealTargetClassesAlreadyPatched() throws Exception {
        // The bundled patches/*.class resources on this branch are themselves the patched build
        // output (PR #157977 is on this branch), so feeding them straight back through transform()
        // exercises the real lazy-loading path (fingerprints.properties + patched byte resources)
        // and should land on the "already patched" no-op branch.
        FsDirectoryFactoryTransformer transformer = new FsDirectoryFactoryTransformer();
        for (String simpleName : new String[] {
            "FsDirectoryFactory",
            "FsDirectoryFactory$1",
            "FsDirectoryFactory$HybridDirectory",
            "FsDirectoryFactory$PreLoadMMapDirectory" }) {
            byte[] classBytes = loadPatchesResource(simpleName + ".class");
            byte[] result = transformer.transform(
                getClass().getClassLoader(),
                "org/elasticsearch/index/store/" + simpleName,
                null,
                null,
                classBytes
            );
            assertThat(simpleName, result, nullValue());
        }
    }

    public void testTransformThrowsForRealTargetClassWithUnknownBytes() throws Exception {
        FsDirectoryFactoryTransformer transformer = new FsDirectoryFactoryTransformer();
        byte[] unknownBytes = "totally unknown bytes".getBytes(StandardCharsets.UTF_8);

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> transformer.transform(
                getClass().getClassLoader(),
                "org/elasticsearch/index/store/FsDirectoryFactory",
                null,
                null,
                unknownBytes
            )
        );
        assertThat(e.getMessage(), containsString(sha256Hex(unknownBytes)));
    }

    public void testSelectReplacementReturnsPatchedBytesOnStockMatch() {
        byte[] stockBytes = "stock class bytes".getBytes(StandardCharsets.UTF_8);
        byte[] patchedBytes = "patched class bytes".getBytes(StandardCharsets.UTF_8);
        Set<String> stockHashes = Set.of(sha256Hex(stockBytes));

        byte[] result = FsDirectoryFactoryTransformer.selectReplacement(
            "org/elasticsearch/index/store/FsDirectoryFactory",
            stockBytes,
            stockHashes,
            patchedBytes
        );

        assertThat(result, sameInstance(patchedBytes));
    }

    public void testSelectReplacementNoOpsWhenAlreadyPatched() {
        byte[] patchedBytes = loadPatchesResource("FsDirectoryFactory$1.class");
        Set<String> stockHashes = Set.of(sha256Hex("some unrelated stock bytes".getBytes(StandardCharsets.UTF_8)));

        byte[] result = FsDirectoryFactoryTransformer.selectReplacement(
            "org/elasticsearch/index/store/FsDirectoryFactory$1",
            patchedBytes.clone(),
            stockHashes,
            patchedBytes
        );

        assertThat(result, nullValue());
    }

    public void testSelectReplacementThrowsOnUnknownFingerprint() {
        byte[] unknownBytes = "totally unknown bytes".getBytes(StandardCharsets.UTF_8);
        byte[] patchedBytes = "patched class bytes".getBytes(StandardCharsets.UTF_8);
        Set<String> stockHashes = Set.of(sha256Hex("some other stock bytes".getBytes(StandardCharsets.UTF_8)));

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> FsDirectoryFactoryTransformer.selectReplacement(
                "org/elasticsearch/index/store/FsDirectoryFactory$HybridDirectory",
                unknownBytes,
                stockHashes,
                patchedBytes
            )
        );
        assertThat(e.getMessage(), containsString(sha256Hex(unknownBytes)));
    }

    private static byte[] loadPatchesResource(String name) {
        try (InputStream in = FsDirectoryFactoryTransformerTests.class.getResourceAsStream("/patches/" + name)) {
            assertNotNull("expected bundled resource /patches/" + name + " to be on the test classpath", in);
            return in.readAllBytes();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(String.format(Locale.ROOT, "%02x", b));
            }
            return hex.toString();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
