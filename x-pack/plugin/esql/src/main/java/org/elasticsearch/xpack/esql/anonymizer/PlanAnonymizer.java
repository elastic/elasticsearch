/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.anonymizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

    public record AnonymizedPlans(String schema, String parsed, String analyzed, String optimized, String physical) {}

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

    /**
     * Anonymize whichever pipeline stages reached completion. Any of the four arguments may be null
     * (e.g. parse failed → all null; analyze failed → only parsed non-null; etc.); the corresponding
     * record field comes back empty. Schema is rendered from analyzed when available, else
     * optimized (the parsed plan has {@code UnresolvedRelation}s with no resolved attributes, so it
     * carries no useful schema).
     */
    public AnonymizedPlans anonymize(LogicalPlan parsed, LogicalPlan analyzed, LogicalPlan optimized, PhysicalPlan physical) {
        String parsedText = parsed == null ? "" : anonymizeLogical(parsed).toString();
        String analyzedText = analyzed == null ? "" : anonymizeLogical(analyzed).toString();
        String optimizedText = optimized == null ? "" : anonymizeLogical(optimized).toString();
        String physicalText = physical == null ? "" : anonymizePhysical(physical).toString();

        LogicalPlan schemaSource = analyzed != null ? analyzed : optimized;
        String schema = schemaSource == null ? "" : renderSchema(schemaSource);

        return new AnonymizedPlans(schema, parsedText, analyzedText, optimizedText, physicalText);
    }

    private LogicalPlan anonymizeLogical(LogicalPlan plan) {
        LogicalPlan out = plan.transformExpressionsDown(Attribute.class, this::anonymizeAttribute);
        out = out.transformExpressionsDown(Literal.class, this::anonymizeLiteral);
        out = out.transformDown(EsRelation.class, this::anonymizeEsRelation);
        // Parsed plans carry UnresolvedRelation (not EsRelation) since analysis hasn't run; the
        // EsRelation rule above doesn't touch it, so handle it explicitly.
        out = out.transformDown(UnresolvedRelation.class, this::anonymizeUnresolvedRelation);
        return out;
    }

    private UnresolvedRelation anonymizeUnresolvedRelation(UnresolvedRelation r) {
        String anonymizedPattern = anonymizeIndex(r.indexPattern().indexPattern());
        return new UnresolvedRelation(
            r.source(),
            new IndexPattern(r.source(), anonymizedPattern),
            r.frozen(),
            r.metadataFields(),
            r.indexMode(),
            null,
            r.telemetryLabel()
        );
    }

    private PhysicalPlan anonymizePhysical(PhysicalPlan plan) {
        PhysicalPlan out = plan.transformExpressionsDown(Attribute.class, this::anonymizeAttribute);
        out = out.transformExpressionsDown(Literal.class, this::anonymizeLiteral);
        // FragmentExec's inner LogicalPlan is not part of the physical tree walk; recurse explicitly.
        out = out.transformDown(FragmentExec.class, fe -> fe.withFragment(anonymizeLogical(fe.fragment())));
        return out;
    }

    private Attribute anonymizeAttribute(Attribute a) {
        String anonymized = anonymizeColumn(a.name());
        // Attribute.withName(...) calls dataType() to reconstruct via clone(); UnresolvedAttribute
        // throws on dataType() because the type hasn't been resolved yet. Build it directly.
        if (a instanceof UnresolvedAttribute) {
            return new UnresolvedAttribute(a.source(), anonymized, null);
        }
        // FieldAttribute keeps a separate parentName that flows through fieldName() and may surface
        // in rendered output for synthetic union-type attributes; anonymize it via the same map so
        // the underlying field reference doesn't leak alongside an anonymized display name.
        if (a instanceof FieldAttribute fa && fa.parentName() != null) {
            String anonParent = anonymizeColumn(fa.parentName());
            return new FieldAttribute(a.source(), anonParent, fa.qualifier(), anonymized, fa.field(), a.nullable(), a.id(), a.synthetic());
        }
        return a.withName(anonymized);
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

    /**
     * Walks each EsRelation in the original (pre-anonymized) plan and emits a schema artifact that
     * carries everything we know about the input mapping — index mode, concrete index names, every
     * field's type plus doc-values / aggregatable / alias / time-series flags, and multifield
     * sub-fields. Identifiers (index names, concrete names, field names, sub-field paths) are
     * anonymized via the same token maps used by the plan rewrite; metadata bits and types are not.
     */
    private String renderSchema(LogicalPlan plan) {
        StringBuilder sb = new StringBuilder();
        plan.forEachDown(EsRelation.class, r -> renderRelation(r, sb));
        return sb.toString();
    }

    private void renderRelation(EsRelation r, StringBuilder sb) {
        sb.append(anonymizeIndex(r.indexPattern())).append(" (mode=").append(r.indexMode().getName());
        if (r.concreteQualifiedIndices().isEmpty() == false) {
            sb.append(", concrete=[");
            boolean first = true;
            for (String concrete : new TreeMap<>(r.indexNameWithModes()).keySet()) {
                if (first == false) {
                    sb.append(", ");
                }
                first = false;
                sb.append(anonymizeIndex(concrete));
            }
            sb.append(']');
        }
        sb.append("):\n");

        Map<String, Attribute> sorted = new TreeMap<>();
        for (Attribute a : r.output()) {
            sorted.put(a.name(), a);
        }
        for (Attribute a : sorted.values()) {
            // Multifield sub-fields (e.g. "job.raw") also appear in output() alongside their parent
            // ("job"). Render them only via the parent's properties descent so each field shows up once.
            if (a.name().indexOf('.') >= 0 && sorted.containsKey(a.name().substring(0, a.name().indexOf('.')))) {
                continue;
            }
            renderAttribute(a, "  ", sb);
        }
    }

    private void renderAttribute(Attribute a, String indent, StringBuilder sb) {
        if (a instanceof FieldAttribute fa) {
            renderField(a.name(), fa.field(), indent, sb);
        } else {
            sb.append(indent).append(anonymizeColumn(a.name())).append(": ").append(a.dataType().typeName());
            if (a.dataType().hasDocValues()) {
                sb.append(" doc_values");
            }
            sb.append('\n');
        }
    }

    private void renderSubField(String fullName, EsField f, String indent, StringBuilder sb) {
        renderField(fullName, f, indent, sb);
    }

    private void renderField(String fullName, EsField f, String indent, StringBuilder sb) {
        sb.append(indent).append(anonymizeColumn(fullName)).append(": ").append(f.getDataType().typeName());
        if (f.getDataType().hasDocValues()) {
            sb.append(" doc_values");
        }
        if (f.isAggregatable()) {
            sb.append(" aggregatable");
        }
        if (f.isAlias()) {
            sb.append(" alias");
        }
        if (f.getTimeSeriesFieldType() != null && f.getTimeSeriesFieldType() != EsField.TimeSeriesFieldType.NONE) {
            sb.append(" ts=").append(f.getTimeSeriesFieldType().name().toLowerCase(Locale.ROOT));
        }
        renderFieldExtras(f, sb);
        sb.append('\n');
        Map<String, EsField> props = f.getProperties();
        if (props != null && props.isEmpty() == false) {
            for (Map.Entry<String, EsField> e : new TreeMap<>(props).entrySet()) {
                renderSubField(fullName + "." + e.getKey(), e.getValue(), indent + "  ", sb);
            }
        }
    }

    /**
     * Subclass-specific {@link EsField} information that the standard flag pass doesn't surface.
     * Keeps the format additive (space-separated {@code key=value}) so older readers can ignore tokens
     * they don't recognize. Index names inside conflict maps are anonymized via the same token map
     * as everywhere else.
     */
    private void renderFieldExtras(EsField f, StringBuilder sb) {
        // Expose the EsField subclass when it carries semantics beyond a plain field. Lets a
        // reviewer distinguish InvalidMappedField / MultiTypeEsField / PotentiallyUnmappedKeywordEsField
        // from a regular field at a glance — per Alex Spies' review.
        String klass = f.getClass().getSimpleName();
        if (klass.equals("EsField") == false) {
            sb.append(" kind=").append(klass);
        }
        if (f instanceof KeywordEsField k) {
            sb.append(" ignore_above=").append(k.getPrecision());
            if (k.getNormalized()) {
                sb.append(" normalized");
            }
        } else if (f instanceof TextEsField t) {
            if (t.getExactInfo().hasExact()) {
                sb.append(" exact_subfield");
            }
        } else if (f instanceof UnsupportedEsField u) {
            if (u.getOriginalTypes() != null && u.getOriginalTypes().isEmpty() == false) {
                sb.append(" original_types=[")
                    .append(String.join(",", new TreeMap<>(toSortedSet(u.getOriginalTypes())).keySet()))
                    .append(']');
            }
            if (u.hasInherited()) {
                sb.append(" inherited_from=").append(anonymizeColumn(u.getInherited()));
            }
        } else if (f instanceof InvalidMappedField imf) {
            Set<DataType> conflictingTypes = imf.types();
            if (conflictingTypes != null && conflictingTypes.isEmpty() == false) {
                Map<String, Boolean> sorted = new TreeMap<>();
                for (DataType t : conflictingTypes) {
                    sorted.put(t.typeName(), Boolean.TRUE);
                }
                sb.append(" type_conflict=[").append(String.join(",", sorted.keySet())).append(']');
            }
        } else if (f instanceof MultiTypeEsField mt) {
            int n = mt.getIndexToConversionExpressions() == null ? 0 : mt.getIndexToConversionExpressions().size();
            sb.append(" multi_type_conversions=").append(n);
        }
    }

    private static <T> Map<T, Boolean> toSortedSet(java.util.Collection<T> in) {
        Map<T, Boolean> m = new TreeMap<>();
        for (T s : in) {
            m.put(s, Boolean.TRUE);
        }
        return m;
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
