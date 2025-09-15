/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.core.util.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.xpack.esql.core.util.PlanStreamInput.readCachedStringWithVersionCheck;
import static org.elasticsearch.xpack.esql.core.util.PlanStreamOutput.writeCachedStringWithVersionCheck;

/**
 * Information about a field in an ES index that cannot be supported by ESQL.
 * All the subfields (properties) of an unsupported type are also be unsupported.
 */
public class UnsupportedEsField extends EsField {

    private final List<String> originalTypes;
    private final String inherited; // for fields belonging to parents (or grandparents) that have an unsupported type

    public UnsupportedEsField(String name, List<String> originalTypes) {
        this(name, originalTypes, null, new TreeMap<>());
    }

    public UnsupportedEsField(String name, List<String> originalTypes, String inherited, Map<String, EsField> properties) {
        super(name, DataType.UNSUPPORTED, properties, false);
        this.originalTypes = originalTypes;
        this.inherited = inherited;
    }

    public UnsupportedEsField(StreamInput in) throws IOException {
        this(readCachedStringWithVersionCheck(in), readOriginalTypes(in), in.readOptionalString(), in.readImmutableMap(EsField::readFrom));
    }

    private static List<String> readOriginalTypes(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_REPORT_ORIGINAL_TYPES_BACKPORT_8_19)) {
            return in.readCollectionAsList(i -> ((PlanStreamInput) i).readCachedString());
        } else {
            return List.of(readCachedStringWithVersionCheck(in).split(","));
        }
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        writeCachedStringWithVersionCheck(out, getName());
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_REPORT_ORIGINAL_TYPES_BACKPORT_8_19)) {
            out.writeCollection(getOriginalTypes(), (o, s) -> ((PlanStreamOutput) o).writeCachedString(s));
        } else {
            writeCachedStringWithVersionCheck(out, String.join(",", getOriginalTypes()));
        }
        out.writeOptionalString(getInherited());
        out.writeMap(getProperties(), (o, x) -> x.writeTo(out));
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
