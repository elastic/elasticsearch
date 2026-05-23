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
    /**
     * Widened from 8 to 12 hex chars (24-bit margin → ~16M unique identifier birthday-collision
     * boundary instead of ~65k at 8 chars). Per-cluster-stable correlation breaks down silently
     * when two distinct field names hash to the same {@code col_xxxxxxxx} on a wide schema, so the
     * extra four chars buy real safety at no rendering-cost penalty.
     */
    private static final int TOKEN_HEX_LEN = 12;

    private final byte[] clusterKey;
    private final Mac mac;
    private final Map<String, String> columnTokens = new HashMap<>();
    private final Map<String, String> indexTokens = new HashMap<>();
    private final Map<LiteralKey, Integer> literalIds = new HashMap<>();

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

    /**
     * Anonymizes a wildcard-style pattern (SQL-like {@code LIKE} / shell {@code KEEP *foo*}). The
     * structural metacharacters {@code *}, {@code ?}, {@code %}, {@code _} survive verbatim so the
     * pattern's shape stays visible; each literal run between metacharacters routes through the
     * column-token map. A backslash escapes the next character, which is treated as literal.
     */
    public String wildcardPattern(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return "";
        }
        StringBuilder out = new StringBuilder(pattern.length() + 16);
        StringBuilder run = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\' && i + 1 < pattern.length()) {
                run.append(pattern.charAt(++i));
                continue;
            }
            if (c == '*' || c == '?' || c == '%' || c == '_') {
                flushRun(out, run);
                out.append(c);
            } else {
                run.append(c);
            }
        }
        flushRun(out, run);
        return out.toString();
    }

    /**
     * Anonymizes a Dissect pattern of the form {@code "%{cap1} sep %{cap2}"}. Capture identifiers
     * inside {@code %{...}} (with or without modifiers {@code ?}, {@code +}) route through the
     * column-token map. Separator characters between captures pass through unchanged — they are
     * usually punctuation, not data.
     */
    public String dissectPattern(String pattern) {
        if (pattern == null) {
            return "";
        }
        return replaceCaptures(pattern, "%{", "}");
    }

    /**
     * Anonymizes a Grok pattern of the form {@code "%{IP:client_ip} %{NUMBER:bytes:int}"}. The
     * Grok-library identifier (the part before the first {@code :}) survives — those are
     * predefined library identifiers (IP, NUMBER, etc.), not customer data. The capture name (after
     * the first {@code :}) routes through the column-token map. Type coercion suffix passes through.
     */
    public String grokPattern(String pattern) {
        if (pattern == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(pattern.length() + 16);
        int i = 0;
        while (i < pattern.length()) {
            int start = pattern.indexOf("%{", i);
            if (start < 0) {
                out.append(pattern, i, pattern.length());
                break;
            }
            out.append(pattern, i, start);
            int end = pattern.indexOf('}', start + 2);
            if (end < 0) {
                out.append(pattern, start, pattern.length());
                break;
            }
            String body = pattern.substring(start + 2, end);
            int firstColon = body.indexOf(':');
            out.append("%{");
            if (firstColon < 0) {
                out.append(body);
            } else {
                String libraryId = body.substring(0, firstColon);
                String rest = body.substring(firstColon + 1);
                int secondColon = rest.indexOf(':');
                String captureName = secondColon < 0 ? rest : rest.substring(0, secondColon);
                String suffix = secondColon < 0 ? "" : rest.substring(secondColon);
                out.append(libraryId).append(':').append(column(captureName)).append(suffix);
            }
            out.append('}');
            i = end + 1;
        }
        return out.toString();
    }

    private String replaceCaptures(String pattern, String open, String close) {
        StringBuilder out = new StringBuilder(pattern.length() + 16);
        int i = 0;
        while (i < pattern.length()) {
            int start = pattern.indexOf(open, i);
            if (start < 0) {
                out.append(pattern, i, pattern.length());
                break;
            }
            out.append(pattern, i, start);
            int end = pattern.indexOf(close, start + open.length());
            if (end < 0) {
                out.append(pattern, start, pattern.length());
                break;
            }
            String body = pattern.substring(start + open.length(), end);
            String modifier = "";
            String captureName = body;
            if (body.startsWith("?") || body.startsWith("+")) {
                modifier = body.substring(0, 1);
                captureName = body.substring(1);
            }
            out.append(open).append(modifier).append(captureName.isEmpty() ? "" : column(captureName)).append(close);
            i = end + close.length();
        }
        return out.toString();
    }

    private void flushRun(StringBuilder out, StringBuilder run) {
        if (run.length() > 0) {
            out.append(column(run.toString()));
            run.setLength(0);
        }
    }

    private String token(String value) {
        byte[] out = mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(out).substring(0, TOKEN_HEX_LEN);
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
