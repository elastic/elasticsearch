/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.anonymizer;

import org.apache.lucene.util.BytesRef;
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
 * one across submissions would leak literal identity across queries.
 */
public final class AnonymizationContext {

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final int TOKEN_HEX_LEN = 8;

    private final byte[] clusterKey;
    private final Map<String, String> columnTokens = new HashMap<>();
    private final Map<String, String> indexTokens = new HashMap<>();
    private final Map<LiteralKey, Integer> literalIds = new HashMap<>();

    private AnonymizationContext(String clusterUuid) {
        this.clusterKey = (clusterUuid == null ? "" : clusterUuid).getBytes(StandardCharsets.UTF_8);
    }

    /** One context per query submission so literal tokens don't carry across queries. */
    public static AnonymizationContext forSubmission(String clusterUuid) {
        return new AnonymizationContext(clusterUuid);
    }

    /** Anonymizes a column / field / alias name. Per-cluster stable. */
    public String column(String name) {
        return columnTokens.computeIfAbsent(name, n -> "col_" + token(n));
    }

    /** Anonymizes an index name, datastream pattern, enrich-policy index, or view name. */
    public String index(String name) {
        return indexTokens.computeIfAbsent(name, n -> "idx_" + token(n));
    }

    /**
     * Anonymizes a literal value of the given {@link DataType}, returning a typed placeholder.
     * Identity within a submission is preserved — repeated literals with the same {(value, type)}
     * key emit the same placeholder.
     */
    public String literal(Object value, DataType type) {
        if (value == null) {
            return "null[" + type + "]";
        }
        int id = literalIds.computeIfAbsent(LiteralKey.of(value, type), k -> literalIds.size());
        if (type == DataType.KEYWORD || type == DataType.TEXT || type == DataType.VERSION || type == DataType.IP) {
            return "L" + id + "[" + type + "]";
        }
        return id + "[" + type + "]";
    }

    private String token(String value) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(clusterKey.length == 0 ? new byte[] { 0 } : clusterKey, HMAC_ALGORITHM));
            byte[] out = mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(out).substring(0, TOKEN_HEX_LEN);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("HMAC-SHA256 unavailable", e);
        }
    }

    private record LiteralKey(DataType type, Object value) {
        static LiteralKey of(Object value, DataType type) {
            Object normalized = value;
            if (value instanceof BytesRef br) {
                normalized = br.utf8ToString();
            } else if (value instanceof List<?> list) {
                normalized = list.toString();
            }
            return new LiteralKey(type, normalized);
        }
    }
}
