/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public record ResolvedEnrichPolicy(
    String matchField,
    String matchType,
    List<String> enrichFields,
    Map<String, String> concreteIndices,
    Map<String, EsField> mapping
) implements Writeable {
    public ResolvedEnrichPolicy(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readString(),
            in.readStringCollectionAsList(),
            in.readMap(StreamInput::readString),
            in.readMap(StreamInput::readString, ResolvedEnrichPolicy::readEsField)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(matchField);
        out.writeString(matchType);
        out.writeStringCollection(enrichFields);
        out.writeMap(concreteIndices, StreamOutput::writeString);
        out.writeMap(mapping, ResolvedEnrichPolicy::writeEsField);
    }

    // TODO: we should have made EsField and DataType Writable, but write it as NamedWritable in PlanStreamInput
    private static void writeEsField(StreamOutput out, EsField field) throws IOException {
        out.writeString(field.getName());
        out.writeString(field.getDataType().typeName());
        out.writeMap(field.getProperties(), ResolvedEnrichPolicy::writeEsField);
        out.writeBoolean(field.isAggregatable());
        out.writeBoolean(field.isAlias());
    }

    private static EsField readEsField(StreamInput in) throws IOException {
        return new EsField(
            in.readString(),
            EsqlDataTypes.fromTypeName(in.readString()),
            in.readMap(ResolvedEnrichPolicy::readEsField),
            in.readBoolean(),
            in.readBoolean()
        );
    }
}
