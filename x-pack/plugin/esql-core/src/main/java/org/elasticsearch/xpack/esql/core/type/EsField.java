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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.core.util.PlanStreamOutput;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.util.PlanStreamInput.readCachedStringWithVersionCheck;
import static org.elasticsearch.xpack.esql.core.util.PlanStreamOutput.writeCachedStringWithVersionCheck;

/**
 * Information about a field in an ES index.
 */
public class EsField implements Writeable {

    /**
     * Fields in a TSDB can be either dimensions or metrics.  This enum provides a way to store, serialize, and operate on those field
     * roles within the ESQL query processing pipeline.
     */
    public enum TimeSeriesFieldType implements Writeable {
        UNKNOWN(0),
        NONE(1),
        METRIC(2),
        DIMENSION(3);

        private final int id;

        TimeSeriesFieldType(int id) {
            this.id = id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(id);
        }

        public static TimeSeriesFieldType readFromStream(StreamInput in) throws IOException {
            int id = in.readVInt();
            return switch (id) {
                case 0 -> UNKNOWN;
                case 1 -> NONE;
                case 2 -> METRIC;
                case 3 -> DIMENSION;
                default -> throw new IOException("Unexpected value for TimeSeriesFieldType: " + id);
            };
        }
    }

    private static Map<String, Reader<? extends EsField>> readers = Map.ofEntries(
        Map.entry("DateEsField", DateEsField::new),
        Map.entry("EsField", EsField::new),
        Map.entry("InvalidMappedField", InvalidMappedField::new),
        Map.entry("KeywordEsField", KeywordEsField::new),
        Map.entry("MultiTypeEsField", MultiTypeEsField::new),
        Map.entry("PotentiallyUnmappedKeywordEsField", PotentiallyUnmappedKeywordEsField::new),
        Map.entry("TextEsField", TextEsField::new),
        Map.entry("UnsupportedEsField", UnsupportedEsField::new)
    );

    public static Reader<? extends EsField> getReader(String name) {
        Reader<? extends EsField> result = readers.get(name);
        if (result == null) {
            throw new IllegalArgumentException("Invalid EsField type [" + name + "]");
        }
        return result;
    }

    private final DataType esDataType;
    private final boolean aggregatable;
    private final Map<String, EsField> properties;
    private final String name;
    private final boolean isAlias;
    // Because the subclasses all reimplement serialization, this needs to be writeable from subclass constructors
    private final TimeSeriesFieldType timeSeriesFieldType;

    public EsField(String name, DataType esDataType, Map<String, EsField> properties, boolean aggregatable) {
        this(name, esDataType, properties, aggregatable, false, TimeSeriesFieldType.UNKNOWN);
    }

    public EsField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean aggregatable,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        this(name, esDataType, properties, aggregatable, false, timeSeriesFieldType);
    }

    public EsField(String name, DataType esDataType, Map<String, EsField> properties, boolean aggregatable, boolean isAlias) {
        this(name, esDataType, properties, aggregatable, isAlias, TimeSeriesFieldType.UNKNOWN);
    }

    public EsField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean aggregatable,
        boolean isAlias,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        this.name = name;
        this.esDataType = esDataType;
        this.aggregatable = aggregatable;
        this.properties = properties;
        this.isAlias = isAlias;
        this.timeSeriesFieldType = timeSeriesFieldType;
    }

    public EsField(StreamInput in) throws IOException {
        this.name = readCachedStringWithVersionCheck(in);
        this.esDataType = readDataType(in);
        this.properties = in.readImmutableMap(EsField::readFrom);
        this.aggregatable = in.readBoolean();
        this.isAlias = in.readBoolean();
        this.timeSeriesFieldType = readTimeSeriesFieldType(in);
    }

    private DataType readDataType(StreamInput in) throws IOException {
        String name = readCachedStringWithVersionCheck(in);
        if (in.getTransportVersion().before(TransportVersions.V_8_16_0) && name.equalsIgnoreCase("NESTED")) {
            /*
             * The "nested" data type existed in older versions of ESQL but was
             * entirely used to filter mappings away. Those versions will still
             * sometimes send it inside EsField when hitting `nested` fields in
             * indices. But the rest of ESQL will never see that type. Thus, we
             * translate it here. We translate to UNSUPPORTED because that seems
             * to work. We've already performed any required filtering.
             */
            return DataType.UNSUPPORTED;
        }
        return DataType.readFrom(name);
    }

    public static <A extends EsField> A readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readEsFieldWithCache();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (((PlanStreamOutput) out).writeEsFieldCacheHeader(this)) {
            writeContent(out);
        }
    }

    /**
     * This needs to be overridden by subclasses for specific serialization
     */
    public void writeContent(StreamOutput out) throws IOException {
        writeCachedStringWithVersionCheck(out, name);
        esDataType.writeTo(out);
        out.writeMap(properties, (o, x) -> x.writeTo(out));
        out.writeBoolean(aggregatable);
        out.writeBoolean(isAlias);
        writeTimeSeriesFieldType(out);
    }

    protected void writeTimeSeriesFieldType(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_SERIALIZE_TIMESERIES_FIELD_TYPE)) {
            this.timeSeriesFieldType.writeTo(out);
        }
    }

    protected static TimeSeriesFieldType readTimeSeriesFieldType(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_SERIALIZE_TIMESERIES_FIELD_TYPE)) {
            return TimeSeriesFieldType.readFromStream(in);
        } else {
            return TimeSeriesFieldType.UNKNOWN;
        }
    }

    /**
     * This needs to be overridden by subclasses for specific serialization
     */
    public String getWriteableName() {
        return "EsField";
    }

    /**
     * Returns the simple name, but not the full field path. The latter requires knowing the path of the parent field.
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
