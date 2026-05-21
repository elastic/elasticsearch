/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.anonymizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Rewrites ES|QL logical and physical plans so customer-sensitive identifiers — column names,
 * index names, literal values — become opaque tokens, while preserving plan shape, data types,
 * attribute identity within one query, and literal identity within one query.
 * <p>
 * Column and index names get a per-cluster stable token via {@code HMAC-SHA256(cluster_uuid, name)}
 * so field-usage telemetry can correlate across queries on the same cluster while leaking nothing
 * across clusters. Literals get a per-submission interning id so the {@code 5} in
 * {@code f == 5 AND bar == 5} resolves to one token, but the same {@code 5} in the next query gets
 * a fresh token — stable literal tokens would carry no telemetry value and would widen
 * frequency-analysis attack surface on common values. Field types are intentionally preserved.
 */
public final class PlanAnonymizer {

    public record AnonymizedPlans(String schema, String logicalPlan, String physicalPlan) {}

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final int TOKEN_HEX_LEN = 8;

    private final byte[] clusterKey;
    private final Map<String, String> columnTokens = new HashMap<>();
    private final Map<String, String> indexTokens = new HashMap<>();
    private final Map<LiteralKey, Integer> literalIds = new HashMap<>();

    private PlanAnonymizer(String clusterUuid) {
        this.clusterKey = clusterUuid.getBytes(StandardCharsets.UTF_8);
    }

    /** One anonymizer per query submission so literal tokens don't carry across queries. */
    public static PlanAnonymizer forSubmission(String clusterUuid) {
        return new PlanAnonymizer(clusterUuid);
    }

    public AnonymizedPlans anonymize(LogicalPlan logical, PhysicalPlan physical) {
        LogicalPlan al = anonymizeLogical(logical);
        PhysicalPlan ap = anonymizePhysical(physical);
        return new AnonymizedPlans(renderSchema(al), al.toString(), ap.toString());
    }

    private LogicalPlan anonymizeLogical(LogicalPlan plan) {
        LogicalPlan out = plan.transformExpressionsDown(Attribute.class, this::anonymizeAttribute);
        out = out.transformExpressionsDown(Literal.class, this::anonymizeLiteral);
        out = out.transformDown(EsRelation.class, this::anonymizeEsRelation);
        return out;
    }

    private PhysicalPlan anonymizePhysical(PhysicalPlan plan) {
        PhysicalPlan out = plan.transformExpressionsDown(Attribute.class, this::anonymizeAttribute);
        out = out.transformExpressionsDown(Literal.class, this::anonymizeLiteral);
        // FragmentExec's inner LogicalPlan is not part of the physical tree walk; recurse explicitly.
        out = out.transformDown(FragmentExec.class, fe -> fe.withFragment(anonymizeLogical(fe.fragment())));
        return out;
    }

    private Attribute anonymizeAttribute(Attribute a) {
        return a.withName(anonymizeColumn(a.name()));
    }

    private Literal anonymizeLiteral(Literal l) {
        if (l.value() == null) {
            return l;
        }
        int id = literalIds.computeIfAbsent(LiteralKey.of(l), k -> literalIds.size());
        return new Literal(l.source(), placeholderFor(l.dataType(), id), l.dataType());
    }

    private EsRelation anonymizeEsRelation(EsRelation r) {
        return new EsRelation(r.source(), anonymizeIndex(r.indexPattern()), r.indexMode(), Map.of(), Map.of(), Map.of(), r.output());
    }

    private String anonymizeColumn(String name) {
        return columnTokens.computeIfAbsent(name, n -> "col_" + token(n));
    }

    private String anonymizeIndex(String pattern) {
        return indexTokens.computeIfAbsent(pattern, p -> "idx_" + token(p));
    }

    private String token(String value) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(clusterKey, HMAC_ALGORITHM));
            byte[] out = mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(out).substring(0, TOKEN_HEX_LEN);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("HMAC-SHA256 unavailable", e);
        }
    }

    private static Object placeholderFor(DataType type, int id) {
        if (type == DataType.KEYWORD || type == DataType.TEXT || type == DataType.VERSION || type == DataType.IP) {
            return new BytesRef("L" + id);
        }
        if (type == DataType.INTEGER || type == DataType.COUNTER_INTEGER) {
            return id;
        }
        if (type == DataType.LONG || type == DataType.COUNTER_LONG || type == DataType.DATETIME || type == DataType.DATE_NANOS) {
            return (long) id;
        }
        if (type == DataType.DOUBLE || type == DataType.COUNTER_DOUBLE) {
            return (double) id;
        }
        if (type == DataType.FLOAT) {
            return (float) id;
        }
        if (type == DataType.BOOLEAN) {
            return id % 2 == 0;
        }
        return (long) id;
    }

    private static String renderSchema(LogicalPlan plan) {
        Map<String, Map<String, String>> byIndex = new TreeMap<>();
        plan.forEachDown(EsRelation.class, r -> {
            Map<String, String> fields = byIndex.computeIfAbsent(r.indexPattern(), k -> new TreeMap<>());
            for (Attribute attr : r.output()) {
                fields.put(attr.name(), attr.dataType().typeName());
            }
        });
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Map<String, String>> idx : byIndex.entrySet()) {
            sb.append(idx.getKey()).append(":\n");
            for (Map.Entry<String, String> f : idx.getValue().entrySet()) {
                sb.append("  ").append(f.getKey()).append(": ").append(f.getValue()).append('\n');
            }
        }
        return sb.toString();
    }

    private record LiteralKey(DataType type, Object value) {
        static LiteralKey of(Literal l) {
            Object v = l.value();
            if (v instanceof BytesRef br) {
                v = br.utf8ToString();
            } else if (v instanceof List<?> list) {
                v = list.toString();
            }
            return new LiteralKey(l.dataType(), v);
        }
    }
}
