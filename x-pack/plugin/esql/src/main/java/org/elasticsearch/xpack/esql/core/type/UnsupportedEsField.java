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
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Information about a field in an ES index that cannot be supported by ESQL.
 * All the subfields (properties) of an unsupported type are also be unsupported.
 */
public class UnsupportedEsField extends EsField {

    private static final TransportVersion ESQL_REPORT_ORIGINAL_TYPES = TransportVersion.fromName("esql_report_original_types");

    private final List<String> originalTypes;
    private final String inherited; // for fields belonging to parents (or grandparents) that have an unsupported type

    public UnsupportedEsField(String name, List<String> originalTypes) {
        this(name, originalTypes, null, new TreeMap<>());
    }

    public UnsupportedEsField(String name, List<String> originalTypes, String inherited, Map<String, EsField> properties) {
        this(name, originalTypes, inherited, properties, TimeSeriesFieldType.UNKNOWN);
    }

    public UnsupportedEsField(
        String name,
        List<String> originalTypes,
        String inherited,
        Map<String, EsField> properties,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        super(name, DataType.UNSUPPORTED, properties, false, timeSeriesFieldType);
        this.originalTypes = originalTypes;
        this.inherited = inherited;
    }

    public UnsupportedEsField(StreamInput in) throws IOException {
        this(
            ((PlanStreamInput) in).readCachedString(),
            readOriginalTypes(in),
            in.readOptionalString(),
            in.readImmutableMap(EsField::readFrom),
            readTimeSeriesFieldType(in)
        );
    }

    private static List<String> readOriginalTypes(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(ESQL_REPORT_ORIGINAL_TYPES)) {
            return in.readCollectionAsList(i -> ((PlanStreamInput) i).readCachedString());
        } else {
            return List.of(((PlanStreamInput) in).readCachedString().split(","));
        }
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        ((PlanStreamOutput) out).writeCachedString(getName());
        if (out.getTransportVersion().supports(ESQL_REPORT_ORIGINAL_TYPES)) {
            out.writeCollection(getOriginalTypes(), (o, s) -> ((PlanStreamOutput) o).writeCachedString(s));
        } else {
            ((PlanStreamOutput) out).writeCachedString(String.join(",", getOriginalTypes()));
        }
        out.writeOptionalString(getInherited());
        out.writeMap(getProperties(), (o, x) -> x.writeTo(out));
        writeTimeSeriesFieldType(out);
    }

    public String getWriteableName() {
        return "UnsupportedEsField";
    }

    public List<String> getOriginalTypes() {
        return originalTypes;
    }

    public String getInherited() {
        return inherited;
    }

    public boolean hasInherited() {
        return inherited != null;
    }

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
        UnsupportedEsField that = (UnsupportedEsField) o;
        return Objects.equals(originalTypes, that.originalTypes) && Objects.equals(inherited, that.inherited);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), originalTypes, inherited);
    }
}
