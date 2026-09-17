/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Information about a field in an es index with the {@code text} type.
 */
public class TextEsField extends EsField {

    // Same transport version as IndexFieldCapabilities#indexAnalyzer. The name is only non-null
    // on a node that also reports that field. Public for tests.
    public static final TransportVersion FIELD_CAPS_INDEX_ANALYZER = TransportVersion.fromName("field_caps_index_analyzer");

    private final @Nullable String analyzerName;

    public TextEsField(
        String name,
        Map<String, EsField> properties,
        boolean hasDocValues,
        boolean isAlias,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        this(name, properties, hasDocValues, isAlias, timeSeriesFieldType, null);
    }

    /**
     * @param analyzerName the analyzer the field is indexed with, as reported by field-caps. {@code null} if the
     *                     field has none, the indices behind the pattern disagree, or a remote predates
     *                     {@link #FIELD_CAPS_INDEX_ANALYZER}. HIGHLIGHT treats {@code null} as {@code standard}.
     */
    public TextEsField(
        String name,
        Map<String, EsField> properties,
        boolean hasDocValues,
        boolean isAlias,
        TimeSeriesFieldType timeSeriesFieldType,
        @Nullable String analyzerName
    ) {
        super(name, TEXT, properties, hasDocValues, isAlias, timeSeriesFieldType);
        this.analyzerName = analyzerName;
    }

    protected TextEsField(StreamInput in) throws IOException {
        this(
            ((PlanStreamInput) in).readCachedString(),
            in.readImmutableMap(EsField::readFrom),
            in.readBoolean(),
            in.readBoolean(),
            readTimeSeriesFieldType(in),
            in.getTransportVersion().supports(FIELD_CAPS_INDEX_ANALYZER) ? in.readOptionalString() : null
        );
    }

    @Override
    public EsField withProperties(Map<String, EsField> newProperties) {
        return new TextEsField(getName(), newProperties, isAggregatable(), isAlias(), getTimeSeriesFieldType(), analyzerName);
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        ((PlanStreamOutput) out).writeCachedString(getName());
        out.writeMap(getProperties(), (o, x) -> x.writeTo(out));
        out.writeBoolean(isAggregatable());
        out.writeBoolean(isAlias());
        writeTimeSeriesFieldType(out);
        if (out.getTransportVersion().supports(FIELD_CAPS_INDEX_ANALYZER)) {
            out.writeOptionalString(analyzerName);
        }
    }

    /**
     * Analyzer the field is indexed with, or {@code null} when unknown.
     */
    public String analyzerName() {
        return analyzerName;
    }

    public String getWriteableName(TransportVersion transportVersion) {
        return "TextEsField";
    }

    @Override
    public EsField getExactField() {
        Tuple<EsField, String> findExact = findExact();
        if (findExact.v1() == null) {
            throw new QlIllegalArgumentException(findExact.v2());
        }
        return findExact.v1();
    }

    @Override
    public Exact getExactInfo() {
        return PROCESS_EXACT_FIELD.apply(findExact());
    }

    private Tuple<EsField, String> findExact() {
        EsField field = null;
        for (EsField property : getProperties().values()) {
            if (property.getDataType() == KEYWORD && property.getExactInfo().hasExact()) {
                if (field != null) {
                    return new Tuple<>(
                        null,
                        "Multiple exact keyword candidates available for [" + getName() + "]; specify which one to use"
                    );
                }
                field = property;
            }
        }
        if (field == null) {
            return new Tuple<>(
                null,
                "No keyword/multi-field defined exact matches for [" + getName() + "]; define one or use MATCH/QUERY instead"
            );
        }
        return new Tuple<>(field, null);
    }

    private Function<Tuple<EsField, String>, Exact> PROCESS_EXACT_FIELD = tuple -> {
        if (tuple.v1() == null) {
            return new Exact(false, tuple.v2());
        } else {
            return new Exact(true, null);
        }
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        TextEsField that = (TextEsField) o;
        return Objects.equals(analyzerName, that.analyzerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), analyzerName);
    }
}
