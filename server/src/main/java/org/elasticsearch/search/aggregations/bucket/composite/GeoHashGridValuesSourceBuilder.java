package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Objects;

public class GeoHashGridValuesSourceBuilder extends CompositeValuesSourceBuilder<GeoHashGridValuesSourceBuilder> {
    static final String TYPE = "geohash_grid";

    private static final ObjectParser<GeoHashGridValuesSourceBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(GeoHashGridValuesSourceBuilder.TYPE);
        PARSER.declareInt(GeoHashGridValuesSourceBuilder::precision, new ParseField("precision"));
        PARSER.declareInt(GeoHashGridValuesSourceBuilder::shardSize, new ParseField("shard_size"));
        CompositeValuesSourceParserHelper.declareValuesSourceFields(PARSER, ValueType.NUMERIC);
    }

    static GeoHashGridValuesSourceBuilder parse(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new GeoHashGridValuesSourceBuilder(name), null);
    }

    private int precision = GeoHashGridAggregationBuilder.DEFAULT_PRECISION;
    private int shardSize = -1;

    GeoHashGridValuesSourceBuilder(String name) {
        super(name);
    }

    GeoHashGridValuesSourceBuilder(StreamInput in) throws IOException {
        super(in);
        this.precision = in.readInt();
        this.shardSize = in.readInt();
    }

    public GeoHashGridValuesSourceBuilder precision(int precision) {
        this.precision = GeoUtils.checkPrecisionRange(precision);
        return this;
    }

    public GeoHashGridValuesSourceBuilder shardSize(int shardSize) {
        this.shardSize = shardSize;
        return this;
    }


    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeInt(precision);
        out.writeInt(shardSize);
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("precision", precision);
        builder.field("shard_size", shardSize);
    }

    @Override
    String type() {
        return TYPE;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        GeoHashGridValuesSourceBuilder other = (GeoHashGridValuesSourceBuilder) obj;
        return Objects.equals(precision, other.precision);
    }


    @Override
    protected CompositeValuesSourceConfig innerBuild(SearchContext context, ValuesSourceConfig<?> config) throws IOException {
        ValuesSource orig = config.toValuesSource(context.getQueryShardContext());
        if (orig == null) {
            orig = ValuesSource.GeoPoint.EMPTY;
        }
        if (orig instanceof ValuesSource.GeoPoint) {
            ValuesSource.GeoPoint geoPoint = (ValuesSource.GeoPoint) orig;
            // is specified in the builder.
            final MappedFieldType fieldType = config.fieldContext() != null ? config.fieldContext().fieldType() : null;
            CellIdSource cellIdSource = new CellIdSource(geoPoint, precision, Geohash::longEncode);
            return new CompositeValuesSourceConfig(name, fieldType, cellIdSource, DocValueFormat.GEOHASH, order(), missingBucket());
        } else {
            throw new IllegalArgumentException("invalid source, expected numeric, got " + orig.getClass().getSimpleName());
        }
    }
}
