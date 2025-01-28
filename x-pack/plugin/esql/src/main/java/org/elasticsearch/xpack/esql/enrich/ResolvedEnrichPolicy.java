/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.type.EsField;

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
            in.readMap(getEsFieldReader(in))
        );
    }

    private static Reader<EsField> getEsFieldReader(StreamInput in) {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_2)) {
            return EsField::readFrom;
        }
        return EsField::new;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(matchField);
        out.writeString(matchType);
        out.writeStringCollection(enrichFields);
        out.writeMap(concreteIndices, StreamOutput::writeString);
        out.writeMap(
            mapping,
            /*
             * There are lots of subtypes of ESField, but we always write the field
             * as though it were the base class.
             */
            (o, v) -> {
                var field = new EsField(v.getName(), v.getDataType(), v.getProperties(), v.isAggregatable(), v.isAlias());
                if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_2)) {
                    field.writeTo(o);
                } else {
                    field.writeContent(o);
                }
            }
        );
    }
}
