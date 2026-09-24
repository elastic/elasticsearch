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
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Information about a field in an es index with the {@code text} type.
 */
public class TextEsField extends EsField {

    /**
     * Same transport version as {@code IndexFieldCapabilities#indexAnalyzer}. Also gates {@link #analyzerGroups}: the
     * HIGHLIGHT analyzer stack ships under one version.
     */
    public static final TransportVersion FIELD_CAPS_INDEX_ANALYZER = TransportVersion.fromName("field_caps_index_analyzer");

    /** {@link TextFieldMapper.Defaults#POSITION_INCREMENT_GAP}, used when {@link #analyzerName} is {@code null}. */
    public static final int DEFAULT_POSITION_INCREMENT_GAP = TextFieldMapper.Defaults.POSITION_INCREMENT_GAP;

    /** Why {@link #analyzerName} is absent. Only meaningful when the name is {@code null}. */
    public enum UnknownAnalyzer {
        /** The name is known, or the field has no analyzer to name. */
        NONE,
        /** Indices disagree on the name or {@code position_increment_gap}. */
        CONFLICT,
        /** Every reported name was withheld because it is defined under {@code index.analysis}. */
        INDEX_LOCAL,
        /**
         * FORK or UNION ALL branches disagree on the mapping of a column they merge, or one computes it. Only set on the
         * mappings HIGHLIGHT carries across a merge, never by field caps.
         */
        BRANCH_CONFLICT
    }

    private final @Nullable String analyzerName;
    private final int positionIncrementGap;
    private final UnknownAnalyzer unknownAnalyzer;
    /** Which indices use which analyzer. Only set on a {@link UnknownAnalyzer#CONFLICT}. */
    private final @Nullable List<IndexAnalyzerGroup> analyzerGroups;

    public TextEsField(
        String name,
        Map<String, EsField> properties,
        boolean hasDocValues,
        boolean isAlias,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        this(
            name,
            properties,
            hasDocValues,
            isAlias,
            timeSeriesFieldType,
            null,
            DEFAULT_POSITION_INCREMENT_GAP,
            UnknownAnalyzer.NONE,
            null
        );
    }

    public TextEsField(
        String name,
        Map<String, EsField> properties,
        boolean hasDocValues,
        boolean isAlias,
        TimeSeriesFieldType timeSeriesFieldType,
        @Nullable String analyzerName,
        int positionIncrementGap,
        UnknownAnalyzer unknownAnalyzer,
        @Nullable List<IndexAnalyzerGroup> analyzerGroups
    ) {
        super(name, TEXT, properties, hasDocValues, isAlias, timeSeriesFieldType);
        assert analyzerName == null || unknownAnalyzer == UnknownAnalyzer.NONE;
        assert analyzerGroups == null || unknownAnalyzer == UnknownAnalyzer.CONFLICT;
        this.analyzerName = analyzerName;
        this.positionIncrementGap = analyzerName == null ? DEFAULT_POSITION_INCREMENT_GAP : positionIncrementGap;
        this.unknownAnalyzer = unknownAnalyzer;
        this.analyzerGroups = analyzerGroups;
    }

    protected TextEsField(StreamInput in) throws IOException {
        this(in, in.getTransportVersion().supports(FIELD_CAPS_INDEX_ANALYZER));
    }

    private TextEsField(StreamInput in, boolean hasAnalyzer) throws IOException {
        this(
            ((PlanStreamInput) in).readCachedString(),
            in.readImmutableMap(EsField::readFrom),
            in.readBoolean(),
            in.readBoolean(),
            readTimeSeriesFieldType(in),
            hasAnalyzer ? in.readOptionalString() : null,
            hasAnalyzer ? in.readVInt() : DEFAULT_POSITION_INCREMENT_GAP,
            hasAnalyzer ? in.readEnum(UnknownAnalyzer.class) : UnknownAnalyzer.NONE,
            hasAnalyzer ? in.readOptionalCollectionAsList(IndexAnalyzerGroup::new) : null
        );
    }

    @Override
    public EsField withProperties(Map<String, EsField> newProperties) {
        return new TextEsField(
            getName(),
            newProperties,
            isAggregatable(),
            isAlias(),
            getTimeSeriesFieldType(),
            analyzerName,
            positionIncrementGap,
            unknownAnalyzer,
            analyzerGroups
        );
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
            // Written even when the name is null; the reader always consumes this vint.
            out.writeVInt(positionIncrementGap);
            out.writeEnum(unknownAnalyzer);
            out.writeOptionalCollection(analyzerGroups);
        }
    }

    public String analyzerName() {
        return analyzerName;
    }

    public int positionIncrementGap() {
        return positionIncrementGap;
    }

    public UnknownAnalyzer unknownAnalyzer() {
        return unknownAnalyzer;
    }

    /** Per-index analyzers when the indices disagree, otherwise {@code null}. */
    public List<IndexAnalyzerGroup> analyzerGroups() {
        return analyzerGroups;
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
        return super.equals(o)
            && o instanceof TextEsField that
            && positionIncrementGap == that.positionIncrementGap
            && unknownAnalyzer == that.unknownAnalyzer
            && Objects.equals(analyzerName, that.analyzerName)
            && Objects.equals(analyzerGroups, that.analyzerGroups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), analyzerName, positionIncrementGap, unknownAnalyzer, analyzerGroups);
    }
}
