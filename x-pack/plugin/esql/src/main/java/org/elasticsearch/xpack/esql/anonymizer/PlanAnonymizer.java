/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.anonymizer;

import org.elasticsearch.xpack.esql.core.anonymizer.AnonymizationContext;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Thin facade over {@link AnonymizationContext}. Each pipeline stage's tree is rendered via
 * {@code plan.toString(NodeStringFormat.FULL, mapper)} where the mapper is the context's
 * {@link NodeStringMapper} view — every {@code nodeString} override that mentions an identifier or
 * literal asks the mapper for the value to emit, and the mapper interns each one via the per-
 * submission token maps. This class also renders the schema artifact (which has its own format
 * requirements separate from the plan toString shape).
 */
public final class PlanAnonymizer {

    public record AnonymizedPlans(String schema, String parsed, String analyzed, String optimized, String physical) {}

    private final AnonymizationContext ctx;
    private final NodeStringMapper mapper;

    private PlanAnonymizer(String clusterUuid) {
        this.ctx = AnonymizationContext.forSubmission(clusterUuid);
        this.mapper = ctx.mapper();
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
        var format = Node.NodeStringFormat.FULL;
        String parsedText = parsed == null ? "" : parsed.toString(format, mapper);
        String analyzedText = analyzed == null ? "" : analyzed.toString(format, mapper);
        String optimizedText = optimized == null ? "" : optimized.toString(format, mapper);
        String physicalText = physical == null ? "" : physical.toString(format, mapper);

        LogicalPlan schemaSource = analyzed != null ? analyzed : optimized;
        String schema = schemaSource == null ? "" : renderSchema(schemaSource);

        return new AnonymizedPlans(schema, parsedText, analyzedText, optimizedText, physicalText);
    }

    /**
     * Walks each {@link EsRelation} in the original (pre-anonymized) plan and emits a schema artifact
     * carrying everything we know about the input mapping — index mode, concrete index names, every
     * field's type plus doc-values / aggregatable / alias / time-series flags, and multifield
     * sub-fields. Identifiers (index names, concrete names, field names, sub-field paths) are
     * anonymized via the per-submission {@link AnonymizationContext}; metadata bits and types are not.
     */
    private String renderSchema(LogicalPlan plan) {
        StringBuilder sb = new StringBuilder();
        plan.forEachDown(EsRelation.class, r -> renderRelation(r, sb));
        return sb.toString();
    }

    private void renderRelation(EsRelation r, StringBuilder sb) {
        sb.append(mapper.index(r.indexPattern())).append(" (mode=").append(r.indexMode().getName());
        if (r.concreteQualifiedIndices().isEmpty() == false) {
            sb.append(", concrete=[");
            boolean first = true;
            for (String concrete : new TreeMap<>(r.indexNameWithModes()).keySet()) {
                if (first == false) {
                    sb.append(", ");
                }
                first = false;
                sb.append(mapper.index(concrete));
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
            // ("job"). Render them via the parent's properties descent so each field shows up once.
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
            sb.append(indent).append(mapper.column(a.name())).append(": ").append(a.dataType().typeName());
            if (a.dataType().hasDocValues()) {
                sb.append(" doc_values");
            }
            sb.append('\n');
        }
    }

    private void renderField(String fullName, EsField f, String indent, StringBuilder sb) {
        sb.append(indent).append(mapper.column(fullName)).append(": ").append(f.getDataType().typeName());
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
                renderField(fullName + "." + e.getKey(), e.getValue(), indent + "  ", sb);
            }
        }
    }

    /**
     * Subclass-specific {@link EsField} information that the standard flag pass doesn't surface.
     * Additive, space-separated {@code key=value} format so older readers can ignore tokens they
     * don't recognize. Inherited-from names route through the column token map.
     */
    private void renderFieldExtras(EsField f, StringBuilder sb) {
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
                    .append(String.join(",", new TreeMap<>(toBoolMap(u.getOriginalTypes())).keySet()))
                    .append(']');
            }
            if (u.hasInherited()) {
                sb.append(" inherited_from=").append(mapper.column(u.getInherited()));
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

    private static <T> Map<T, Boolean> toBoolMap(java.util.Collection<T> in) {
        Map<T, Boolean> m = new TreeMap<>();
        for (T s : in) {
            m.put(s, Boolean.TRUE);
        }
        return m;
    }
}
