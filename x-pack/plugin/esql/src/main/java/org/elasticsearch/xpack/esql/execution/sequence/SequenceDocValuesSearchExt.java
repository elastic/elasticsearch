/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/**
 * Carries the resolved mapped ES|QL projection through a sequence's PIT search. Ordinary ES|QL has no Search API
 * docvalue-field count limit; this internal extension preserves that behavior while using the ordinary mapped block
 * loaders for every storage layout. REST requests cannot construct this extension.
 */
public final class SequenceDocValuesSearchExt extends SearchExtBuilder {
    public static final String NAME = "esql_sequence_doc_values";

    private final List<FieldAndFormat> fields;
    private final MappedFieldType.FieldExtractPreference fieldExtractPreference;

    /** Deduplicates the resolved physical projection; loaded values retain their native compute representation. */
    public SequenceDocValuesSearchExt(List<FieldAndFormat> fields) {
        this(fields, MappedFieldType.FieldExtractPreference.NONE);
    }

    /** Retains the query's extraction preference so ordinary ES|QL and sequence reads select the same storage layout. */
    public SequenceDocValuesSearchExt(List<FieldAndFormat> fields, MappedFieldType.FieldExtractPreference fieldExtractPreference) {
        // A physical field can supply several aliases in the raw schema, but must only be fetched once.
        var unique = new LinkedHashMap<String, FieldAndFormat>();
        for (FieldAndFormat field : fields) {
            unique.put(field.field, field);
        }
        this.fields = List.copyOf(unique.values());
        this.fieldExtractPreference = Objects.requireNonNull(fieldExtractPreference);
    }

    /** Restores an internal projection only on nodes supporting the native sequence command. */
    public SequenceDocValuesSearchExt(StreamInput in) throws IOException {
        Sequence.checkSupported(in.getTransportVersion());
        fields = in.readCollectionAsImmutableList(FieldAndFormat::new);
        fieldExtractPreference = in.readEnum(MappedFieldType.FieldExtractPreference.class);
    }

    /** Rejects public search requests rather than exposing an alternative to their configured field limit. */
    public static SequenceDocValuesSearchExt fromXContent(XContentParser parser) {
        throw new XContentParseException(parser.getTokenLocation(), "[" + NAME + "] is reserved for internal ES|QL searches");
    }

    public List<FieldAndFormat> fields() {
        return fields;
    }

    public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
        return fieldExtractPreference;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return Sequence.ESQL_SEQUENCE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Sequence.checkSupported(out.getTransportVersion());
        out.writeCollection(fields);
        out.writeEnum(fieldExtractPreference);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("fields", fields);
        builder.field("field_extract_preference", fieldExtractPreference.name());
        return builder.endObject();
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj
            || obj instanceof SequenceDocValuesSearchExt other
                && fields.equals(other.fields)
                && fieldExtractPreference == other.fieldExtractPreference;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, fieldExtractPreference);
    }
}
