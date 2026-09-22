/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IndexModeFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xpack.cluster.routing.allocation.mapper.DataTierFieldMapper;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

public final class MetadataAttribute extends TypedAttribute {
    public static final String TIMESTAMP_FIELD = "@timestamp"; // this is not a true metadata attribute
    public static final String TSID_FIELD = "_tsid";
    public static final String SCORE = "_score";
    public static final String INDEX = "_index";
    public static final String TIMESERIES = "_timeseries";
    public static final String SIZE = "_size";
    // The kind of ES relation a row came from and that relation's own name. Named RELATION_* rather
    // than CLASS/NAME because MetadataAttribute.NAME would read as the name of the attribute rather
    // than of the relation it describes. The values _class can take are a closed set, one per
    // relation kind: see RelationClass.
    public static final String RELATION_CLASS = "_class";
    public static final String RELATION_NAME = "_name";
    public static final String DOC = "_doc";

    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "MetadataAttribute",
        MetadataAttribute::readFrom
    );

    public static final Map<String, MetadataAttributeConfiguration> ATTRIBUTES_MAP = createMetadataAttributes();

    @SuppressWarnings("unchecked")
    private static Map<String, MetadataAttributeConfiguration> createMetadataAttributes() {
        var entries = new ArrayList<Map.Entry<String, MetadataAttributeConfiguration>>();
        entries.add(Map.entry("_version", new MetadataAttributeConfiguration(DataType.LONG, false)));
        entries.add(Map.entry(INDEX, new MetadataAttributeConfiguration(DataType.KEYWORD, true)));
        // actually _id is searchable, but fielddata access on it is disallowed by default
        entries.add(Map.entry(IdFieldMapper.NAME, new MetadataAttributeConfiguration(DataType.KEYWORD, false)));
        entries.add(Map.entry(IgnoredFieldMapper.NAME, new MetadataAttributeConfiguration(DataType.KEYWORD, true)));
        entries.add(Map.entry(SourceFieldMapper.NAME, new MetadataAttributeConfiguration(DataType.SOURCE, false)));
        entries.add(Map.entry(IndexModeFieldMapper.NAME, new MetadataAttributeConfiguration(DataType.KEYWORD, true)));
        entries.add(Map.entry(SCORE, new MetadataAttributeConfiguration(DataType.DOUBLE, false)));
        entries.add(Map.entry(TSID_FIELD, new MetadataAttributeConfiguration(DataType.TSID_DATA_TYPE, false)));
        // Searchable field added by the mapper-size plugin.
        // See https://www.elastic.co/docs/reference/elasticsearch/plugins/mapper-size-usage
        entries.add(Map.entry(SIZE, new MetadataAttributeConfiguration(DataType.INTEGER, true)));
        // Not searchable: no Lucene field backs either name on an index. Nothing actually consults this
        // for them, because MaterializeRelationClassAndName replaces both attributes with references over
        // an Eval during logical optimization, long before LucenePushdownPredicates -- the only reader of
        // this flag -- runs. That rule is what keeps a filter on either name off the shards. False is both
        // the honest value for a field no index stores and the safe one if the rule is ever skipped.
        entries.add(Map.entry(RELATION_CLASS, new MetadataAttributeConfiguration(DataType.KEYWORD, false)));
        entries.add(Map.entry(RELATION_NAME, new MetadataAttributeConfiguration(DataType.KEYWORD, false)));
        if (EsqlCapabilities.Cap.METADATA_TIER_FIELD.isEnabled()) {
            entries.add(Map.entry(DataTierFieldMapper.NAME, new MetadataAttributeConfiguration(DataType.KEYWORD, true)));
        }
        if (EsqlCapabilities.Cap.METADATA_SLICE.isEnabled()) {
            // _slice is the virtual routing alias used by slice-enabled indices. Backed by _routing sorted doc values.
            entries.add(Map.entry(SliceIndexing.FIELD_NAME, new MetadataAttributeConfiguration(DataType.KEYWORD, true)));
        }
        return Map.ofEntries(entries.toArray(Map.Entry[]::new));
    }

    private record MetadataAttributeConfiguration(DataType dataType, boolean searchable) {}

    private final boolean searchable;

    public MetadataAttribute(
        Source source,
        String name,
        DataType dataType,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic,
        boolean searchable
    ) {
        super(source, name, dataType, nullability, id, synthetic);
        this.searchable = searchable;
    }

    public MetadataAttribute(Source source, String name, DataType dataType, boolean searchable) {
        this(source, name, dataType, Nullability.TRUE, null, false, searchable);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (((PlanStreamOutput) out).writeAttributeCacheHeader(this)) {
            Source.EMPTY.writeTo(out);
            out.writeString(name());
            dataType().writeTo(out);
            out.writeOptionalString(null); // qualifier, no longer used
            out.writeEnum(nullable());
            id().writeTo(out);
            out.writeBoolean(synthetic());
            out.writeBoolean(searchable);
        }
    }

    public static MetadataAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(stream -> {
            Source source = Source.readFrom((PlanStreamInput) stream);
            String name = stream.readString();
            DataType dataType = DataType.readFrom(stream);
            String qualifier = stream.readOptionalString(); // qualifier, no longer used
            Nullability nullability = stream.readEnum(Nullability.class);
            NameId id = NameId.readFrom((PlanStreamInput) stream);
            boolean synthetic = stream.readBoolean();
            boolean searchable = stream.readBoolean();
            return new MetadataAttribute(source, name, dataType, nullability, id, synthetic, searchable);
        });
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected MetadataAttribute clone(
        Source source,
        String qualifier,
        String name,
        DataType type,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        // Ignores qualifier, as metadata attributes do not have qualifiers.
        return new MetadataAttribute(source, name, type, nullability, id, synthetic, searchable);
    }

    @Override
    protected String label() {
        return "m";
    }

    @Override
    public boolean isDimension() {
        // Metadata attributes cannot be dimensions. I think?
        return false;
    }

    @Override
    public boolean isMetric() {
        // Metadata attributes definitely cannot be metrics.
        return false;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MetadataAttribute::new, name(), dataType(), nullable(), id(), synthetic(), searchable);
    }

    public boolean searchable() {
        return searchable;
    }

    public static NamedExpression create(Source source, String name) {
        var t = ATTRIBUTES_MAP.get(name);
        if (t != null) {
            return new MetadataAttribute(source, name, t.dataType(), t.searchable());
        }

        return new UnresolvedMetadataAttributeExpression(source, name);
    }

    /**
     * The {@code METADATA} clause name of {@code requested}. {@link #create} returns a
     * {@link MetadataAttribute} for names in {@link #ATTRIBUTES_MAP} and an
     * {@link UnresolvedMetadataAttributeExpression} otherwise; the latter's {@link #name()} throws.
     * The {@code EXTERNAL} shim uses a plain {@link UnresolvedAttribute}, so both unresolved shapes
     * occur.
     */
    public static String metadataName(NamedExpression requested) {
        return requested instanceof UnresolvedMetadataAttributeExpression unr ? unr.pattern() : requested.name();
    }

    public static DataType dataType(String name) {
        var t = ATTRIBUTES_MAP.get(name);
        return t != null ? t.dataType() : null;
    }

    public static boolean isSupported(String name) {
        return ATTRIBUTES_MAP.containsKey(name);
    }

    public static boolean isScoreAttribute(Expression a) {
        return a instanceof MetadataAttribute ma && ma.name().equals(SCORE);
    }

    public static boolean isTimeSeriesAttributeName(String name) {
        return TIMESERIES.equals(name);
    }

    public static boolean isTimeSeriesAttribute(Expression a) {
        return a instanceof TimeSeriesMetadataAttribute || a instanceof NamedExpression named && isTimeSeriesAttributeName(named.name());
    }

    @Override
    protected int innerHashCode(boolean ignoreIds) {
        return Objects.hash(super.innerHashCode(ignoreIds), searchable);
    }

    @Override
    protected boolean innerEquals(Object o, boolean ignoreIds) {
        var other = (MetadataAttribute) o;
        return super.innerEquals(other, ignoreIds) && searchable == other.searchable;
    }
}
