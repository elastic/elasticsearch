/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase.StorageBackend;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Defines the bounded parameter matrix and deterministic guard cells for one data-source BWC suite.
 *
 * <p>The owning test keeps this policy beside its parameter factory. Gradle supplies only mixed-cluster
 * facts and does not need to know about storage backends, codecs, or representative csv-spec tests.
 */
public record BwcMatrixPolicy(
    StorageBackend corpusBackend,
    @Nullable String corpusCodec,
    List<BwcTestId> representatives,
    Set<StorageBackend> guardBackends
) {

    public BwcMatrixPolicy {
        Objects.requireNonNull(corpusBackend, "corpusBackend");
        corpusCodec = corpusCodec == null ? null : EsqlDataSourceCodecEligibility.normalizeCodecToken(corpusCodec);
        representatives = List.copyOf(Objects.requireNonNull(representatives, "representatives"));
        if (representatives.isEmpty()) {
            throw new IllegalArgumentException("At least one BWC representative is required");
        }
        if (representatives.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("BWC representatives must not contain null");
        }
        guardBackends = Set.copyOf(Objects.requireNonNull(guardBackends, "guardBackends"));
        for (StorageBackend backend : guardBackends) {
            if (backend.supportsMultiFileGuard() == false) {
                throw new IllegalArgumentException("Storage backend [" + backend + "] does not support the multi-file BWC guard");
            }
        }
    }

    /** Creates an uncompressed policy using all glob-capable backends for deterministic guards. */
    public static BwcMatrixPolicy uncompressed(StorageBackend corpusBackend, BwcTestId... representatives) {
        return new BwcMatrixPolicy(corpusBackend, null, requiredRepresentatives(representatives), defaultGuardBackends());
    }

    /** Creates a compressed policy using all glob-capable backends for deterministic guards. */
    public static BwcMatrixPolicy compressed(StorageBackend corpusBackend, String corpusCodec, BwcTestId... representatives) {
        if (corpusCodec == null || corpusCodec.isBlank()) {
            throw new IllegalArgumentException("A compressed BWC policy requires a non-blank corpus codec");
        }
        return new BwcMatrixPolicy(corpusBackend, corpusCodec, requiredRepresentatives(representatives), defaultGuardBackends());
    }

    /** Returns this policy with an explicitly reduced set of glob-capable guard backends. */
    public BwcMatrixPolicy withGuardBackends(Set<StorageBackend> guardBackends) {
        return new BwcMatrixPolicy(corpusBackend, corpusCodec, representatives, guardBackends);
    }

    /**
     * Whether a parameter cell carries a deterministic guard.
     *
     * <p>Compressed suites guard every eligible codec on the corpus backend and the corpus codec on
     * every selected guard backend. Uncompressed suites guard {@code none} on every selected backend.
     */
    public boolean isGuardCell(StorageBackend backend, String normalizedCodec) {
        if (backend == null || normalizedCodec == null) {
            return false;
        }
        String codec = EsqlDataSourceCodecEligibility.normalizeCodecToken(normalizedCodec);
        if (corpusCodec == null) {
            return guardBackends.contains(backend) && "none".equals(codec);
        }
        return backend == corpusBackend || (guardBackends.contains(backend) && corpusCodec.equals(codec));
    }

    /** Codec identity used by an uncompressed or compressed corpus guard. */
    public String corpusGuardCodecIdentity() {
        return corpusCodec == null ? "none" : corpusCodec;
    }

    private static List<BwcTestId> requiredRepresentatives(BwcTestId[] representatives) {
        Objects.requireNonNull(representatives, "representatives");
        if (representatives.length == 0) {
            throw new IllegalArgumentException("At least one BWC representative is required");
        }
        return Arrays.asList(representatives);
    }

    private static Set<StorageBackend> defaultGuardBackends() {
        return Arrays.stream(StorageBackend.values())
            .filter(StorageBackend::supportsMultiFileGuard)
            .collect(Collectors.toUnmodifiableSet());
    }

    /** Identifies a csv-spec test that can represent non-corpus matrix cells. */
    public record BwcTestId(String fileName, String testName) {
        public BwcTestId {
            if (fileName == null || fileName.isBlank() || testName == null || testName.isBlank()) {
                throw new IllegalArgumentException("A BWC test id requires a file name and test name");
            }
        }
    }
}
