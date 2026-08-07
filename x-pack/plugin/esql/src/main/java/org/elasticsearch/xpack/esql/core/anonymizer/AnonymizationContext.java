/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.anonymizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Per-query-submission state carried through a single {@code anonymize()} traversal of a plan tree.
 * Holds the HMAC key (derived from the cluster identifier) plus the token maps that intern each
 * identifier and literal so a given input maps to the same token across every appearance in the
 * tree.
 * <p>
 * Two scopes of stability:
 * <ul>
 *   <li>Column, index, enrich and lookup names get a per-cluster-stable token via
 *       {@code HMAC-SHA256(cluster_uuid, name)}. Same name on the same cluster yields the same
 *       token across queries — useful for cross-incident field-usage telemetry. Disjoint across
 *       clusters by construction.</li>
 *   <li>Literals get a per-submission interning id so identity within one query is preserved
 *       (the two {@code 5}s in {@code f == 5 AND bar == 5} share a token) but the same {@code 5}
 *       gets a fresh token in the next query.</li>
 * </ul>
 * A fresh context is constructed per query submission via {@link #forSubmission(String)}; reusing
 * one across submissions would leak literal identity across queries. Used entirely within a single
 * thread, so its plain {@link HashMap} state needs no synchronization.
 */
public final class AnonymizationContext {

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    /**
     * Length of each hashed identifier. We use 12 because it takes ~16M entries before we're 50/50
     * to hit a collision (two field names hashing to the same {@code col_xxxxxxxx}).
     */
    private static final int TOKEN_HEX_LEN = 12;

    private final byte[] clusterKey;
    private final Mac mac;
    private final Map<String, String> columnTokens = new HashMap<>();
    private final Map<String, String> indexTokens = new HashMap<>();
    private final Map<LiteralKey, Integer> literalIds = new HashMap<>();
    private final NodeStringMapper mapper = new NodeStringMapper() {
        @Override
        public String column(String name) {
            return columnTokens.computeIfAbsent(name, n -> "col_" + token(n));
        }

        @Override
        public String index(String name) {
            return indexTokens.computeIfAbsent(name, n -> "idx_" + token(n));
        }

        @Override
        public String literal(Object value, DataType type) {
            if (value == null) {
                return "null";
            }
            // size() gives the next sequential id (0, 1, 2, ...) as each new literal is interned.
            int id = literalIds.computeIfAbsent(LiteralKey.of(value, type), k -> literalIds.size());
            if (type == DataType.KEYWORD || type == DataType.TEXT || type == DataType.VERSION || type == DataType.IP) {
                return "L" + id;
            }
            return String.valueOf(id);
        }

        @Override
        public String opaque(String text) {
            // Free-form text (raw query DSL, sort/stats descriptors, external source paths) can embed
            // identifiers in unpredictable positions; redact the whole fragment rather than risk a
            // partial leak. The field stays in place so the plan shape is unchanged.
            return "<redacted>";
        }
    };

    private AnonymizationContext(String clusterUuid) {
        this.clusterKey = (clusterUuid == null ? "" : clusterUuid).getBytes(StandardCharsets.UTF_8);
        // One Mac instance per submission, reused across every token() call. Mac is not
        // thread-safe but AnonymizationContext is constructed per submission and used single-
        // threadedly, so caching saves the Mac.getInstance() + SecretKeySpec allocations per
        // identifier render — non-trivial on wide schemas.
        try {
            this.mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(clusterKey.length == 0 ? new byte[] { 0 } : clusterKey, HMAC_ALGORITHM));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("HMAC-SHA256 unavailable", e);
        }
    }

    /** One context per query submission so literal tokens don't carry across queries. */
    public static AnonymizationContext forSubmission(String clusterUuid) {
        return new AnonymizationContext(clusterUuid);
    }

    /**
     * Returns the {@link NodeStringMapper} view backed by this context's state. The mapper is a
     * function — pass it to {@code nodeString} / {@code toString}. The context holds the lifecycle;
     * the mapper exposes the pure-function surface call sites consume.
     */
    public NodeStringMapper mapper() {
        return mapper;
    }

    private String token(String value) {
        byte[] out = mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(out).substring(0, TOKEN_HEX_LEN);
    }

    private record LiteralKey(DataType type, Object value) {
        static LiteralKey of(Object value, DataType type) {
            Object normalized = value;
            if (value instanceof BytesRef br) {
                // A stable, content-equal copy detached from any shared/mutable backing buffer.
                // BytesRef has content-based equals/hashCode, so this keys correctly for every
                // BytesRef-backed type — including spatial WKB, where utf8ToString would garble.
                normalized = BytesRef.deepCopyOf(br);
            } else if (value instanceof List<?> list) {
                normalized = list.toString();
            }
            return new LiteralKey(type, normalized);
        }
    }
}
