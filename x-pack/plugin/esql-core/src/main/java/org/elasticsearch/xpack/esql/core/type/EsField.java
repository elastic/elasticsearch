/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Information about a field in an ES index.
 */
public class EsField implements NamedWriteable {
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            EsField.ENTRY,
            DateEsField.ENTRY,
            InvalidMappedField.ENTRY,
            KeywordEsField.ENTRY,
            TextEsField.ENTRY,
            UnsupportedEsField.ENTRY
        );
    }

    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(EsField.class, "EsField", EsField::new);

    private final DataType esDataType;
    private final boolean aggregatable;
    private final Map<String, EsField> properties;
    private final String name;
    private final boolean isAlias;

    public EsField(String name, DataType esDataType, Map<String, EsField> properties, boolean aggregatable) {
        this(name, esDataType, properties, aggregatable, false);
    }

    public EsField(String name, DataType esDataType, Map<String, EsField> properties, boolean aggregatable, boolean isAlias) {
        this.name = name;
        this.esDataType = esDataType;
        this.aggregatable = aggregatable;
        this.properties = properties;
        this.isAlias = isAlias;
    }

    public EsField(StreamInput in) throws IOException {
        this.name = in.readString();
        this.esDataType = DataType.readFrom(in);
        this.properties = in.readImmutableMap(i -> i.readNamedWriteable(EsField.class));
        this.aggregatable = in.readBoolean();
        this.isAlias = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(esDataType.typeName());
        out.writeMap(properties, StreamOutput::writeNamedWriteable);
        out.writeBoolean(aggregatable);
        out.writeBoolean(isAlias);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    /**
     * Returns the field path
     */
    public String getName() {
        return name;
    }

    /**
     * The field type
     */
    public DataType getDataType() {
        return esDataType;
    }

    /**
     * This field can be aggregated
     */
    public boolean isAggregatable() {
        return aggregatable;
    }

    /**
     * Returns list of properties for the nested and object fields, list of subfield if the field
     * was indexed in a few different ways or null otherwise
     */
    @Nullable
    public Map<String, EsField> getProperties() {
        return properties;
    }

    /**
     * This field is an alias to another field
     */
    public boolean isAlias() {
        return isAlias;
    }

    /**
     * Returns the path to the keyword version of this field if this field is text and it has a subfield that is
     * indexed as keyword, throws an exception if such field is not found or the field name itself in all other cases.
     * To avoid the exception {@link EsField#getExactInfo()} should be used beforehand, to check if an exact field exists
     * and if not get the errorMessage which explains why is that.
     */
    public EsField getExactField() {
        return this;
    }

    /**
     * Returns and {@link Exact} object with all the necessary info about the field:
     * <ul>
     *  <li>If it has an exact underlying field or not</li>
     *  <li>and if not an error message why it doesn't</li>
     * </ul>
     */
    public Exact getExactInfo() {
        return Exact.EXACT_FIELD;
    }

    @Override
    public String toString() {
        return name + "@" + esDataType.typeName() + "=" + properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EsField field = (EsField) o;
        return aggregatable == field.aggregatable
            && isAlias == field.isAlias
            && esDataType == field.esDataType
            && Objects.equals(name, field.name)
            && Objects.equals(properties, field.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(esDataType, aggregatable, properties, name, isAlias);
    }

    public static final class Exact {

        private static Exact EXACT_FIELD = new Exact(true, null);

        private boolean hasExact;
        private String errorMsg;

        public Exact(boolean hasExact, String errorMsg) {
            this.hasExact = hasExact;
            this.errorMsg = errorMsg;
        }

        public boolean hasExact() {
            return hasExact;
        }

        public String errorMsg() {
            return errorMsg;
        }
    }
}
