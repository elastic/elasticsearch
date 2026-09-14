/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.patch;

import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Substitutes the bundled patched bytecode for the 4 pre-existing {@code FsDirectoryFactory*}
 * classes, gated on a SHA-256 fingerprint match against the known stock (unpatched) class files.
 */
final class FsDirectoryFactoryTransformer implements ClassFileTransformer {

    private static final String PACKAGE_INTERNAL_NAME = "org/elasticsearch/index/store/";

    private static final Set<String> TARGET_CLASSES = Set.of(
        PACKAGE_INTERNAL_NAME + "FsDirectoryFactory",
        PACKAGE_INTERNAL_NAME + "FsDirectoryFactory$1",
        PACKAGE_INTERNAL_NAME + "FsDirectoryFactory$HybridDirectory",
        PACKAGE_INTERNAL_NAME + "FsDirectoryFactory$PreLoadMMapDirectory"
    );

    // The bundled resources are read-only for the life of the JVM and tiny, so they are loaded once
    // when this class is initialized (during premain, when the agent constructs the transformer).
    // A missing or unreadable resource therefore fails fast at attach time rather than at first
    // target-class load.
    private static final Map<String, Set<String>> STOCK_FINGERPRINTS = loadFingerprints();
    private static final Map<String, byte[]> PATCHED_BYTES = loadPatchedBytes();

    @Override
    public byte[] transform(
        ClassLoader loader,
        String className,
        Class<?> classBeingRedefined,
        ProtectionDomain protectionDomain,
        byte[] classfileBuffer
    ) throws IllegalClassFormatException {
        if (className == null || TARGET_CLASSES.contains(className) == false) {
            return null;
        }

        String simpleName = className.substring(PACKAGE_INTERNAL_NAME.length());
        return selectReplacement(className, classfileBuffer, STOCK_FINGERPRINTS.get(simpleName), PATCHED_BYTES.get(simpleName));
    }

    /**
     * Pure decision logic, isolated from resource loading so it can be unit tested with
     * synthetic fingerprints and bytes.
     */
    static byte[] selectReplacement(String className, byte[] classfileBuffer, Set<String> stockHashes, byte[] patchedBytes) {
        if (stockHashes == null || patchedBytes == null) {
            throw new IllegalStateException("[fs-patch-agent] No bundled fingerprint or patch for " + className);
        }

        String sha = sha256Hex(classfileBuffer);

        if (stockHashes.contains(sha)) {
            System.out.println("[fs-patch-agent] Patched " + className);
            return patchedBytes;
        }
        if (Arrays.equals(classfileBuffer, patchedBytes)) {
            return null;
        }
        throw new IllegalStateException("[fs-patch-agent] Unknown FsDirectoryFactory fingerprint for " + className + ": " + sha);
    }

    private static Map<String, Set<String>> loadFingerprints() {
        Properties properties = new Properties();
        try (InputStream in = FsDirectoryFactoryTransformer.class.getResourceAsStream("/fingerprints.properties")) {
            if (in == null) {
                throw new IllegalStateException("[fs-patch-agent] Missing bundled resource /fingerprints.properties");
            }
            properties.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("[fs-patch-agent] Failed to load bundled resource /fingerprints.properties", e);
        }

        Map<String, Set<String>> fingerprints = new HashMap<>();
        for (String name : properties.stringPropertyNames()) {
            fingerprints.put(name, new HashSet<>(Arrays.asList(properties.getProperty(name).split(","))));
        }
        return fingerprints;
    }

    private static Map<String, byte[]> loadPatchedBytes() {
        Map<String, byte[]> patchedBytes = new HashMap<>();
        for (String internalName : TARGET_CLASSES) {
            String simpleName = internalName.substring(PACKAGE_INTERNAL_NAME.length());
            patchedBytes.put(simpleName, loadResource("/patches/" + simpleName + ".class"));
        }
        return patchedBytes;
    }

    private static byte[] loadResource(String resourcePath) {
        try (InputStream in = FsDirectoryFactoryTransformer.class.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("[fs-patch-agent] Missing bundled resource " + resourcePath);
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new IllegalStateException("[fs-patch-agent] Failed to load bundled resource " + resourcePath, e);
        }
    }

    private static String sha256Hex(byte[] bytes) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        byte[] hash = digest.digest(bytes);
        StringBuilder hex = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            hex.append(String.format(Locale.ROOT, "%02x", b));
        }
        return hex.toString();
    }
}
