package org.elasticsearch.index.mapper.geo;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJSONShapeParser;
import org.elasticsearch.common.geo.GeoShapeConstants;
import org.elasticsearch.common.lucene.spatial.SpatialStrategy;
import org.elasticsearch.common.lucene.spatial.prefix.TermQueryPrefixTreeStrategy;
import org.elasticsearch.common.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.elasticsearch.common.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.elasticsearch.common.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.Map;

/**
 * FieldMapper for indexing {@link com.spatial4j.core.shape.Shape}s.
 * <p/>
 * Currently Shapes can only be indexed and can only be queried using
 * {@link org.elasticsearch.index.query.GeoShapeFilterParser}, consequently
 * a lot of behavior in this Mapper is disabled.
 * <p/>
 * Format supported:
 * <p/>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 * ]
 * }
 */
public class GeoShapeFieldMapper extends AbstractFieldMapper<String> {

    public static final String CONTENT_TYPE = "geo_shape";

    public static class Names {
        public static final String TREE = "tree";
        public static final String TREE_LEVELS = "tree_levels";
        public static final String GEOHASH = "geohash";
        public static final String QUADTREE = "quadtree";
        public static final String DISTANCE_ERROR_PCT = "distance_error_pct";
    }

    public static class Defaults {
        public static final String TREE = Names.GEOHASH;
        public static final int GEOHASH_LEVELS = GeohashPrefixTree.getMaxLevelsPossible();
        public static final int QUADTREE_LEVELS = QuadPrefixTree.DEFAULT_MAX_LEVELS;
        public static final double DISTANCE_ERROR_PCT = 0.025d;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexed(true);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, GeoShapeFieldMapper> {

        private String tree = Defaults.TREE;
        private int treeLevels;
        private double distanceErrorPct = Defaults.DISTANCE_ERROR_PCT;

        private SpatialPrefixTree prefixTree;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
        }

        public Builder tree(String tree) {
            this.tree = tree;
            return this;
        }

        public Builder treeLevels(int treeLevels) {
            this.treeLevels = treeLevels;
            return this;
        }

        public Builder distanceErrorPct(double distanceErrorPct) {
            this.distanceErrorPct = distanceErrorPct;
            return this;
        }

        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            if (tree.equals(Names.GEOHASH)) {
                int levels = treeLevels != 0 ? treeLevels : Defaults.GEOHASH_LEVELS;
                prefixTree = new GeohashPrefixTree(GeoShapeConstants.SPATIAL_CONTEXT, levels);
            } else if (tree.equals(Names.QUADTREE)) {
                int levels = treeLevels != 0 ? treeLevels : Defaults.QUADTREE_LEVELS;
                prefixTree = new QuadPrefixTree(GeoShapeConstants.SPATIAL_CONTEXT, levels);
            } else {
                throw new ElasticSearchIllegalArgumentException("Unknown prefix tree type [" + tree + "]");
            }

            return new GeoShapeFieldMapper(buildNames(context), prefixTree, distanceErrorPct, fieldType, provider);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (Names.TREE.equals(fieldName)) {
                    builder.tree(fieldNode.toString());
                } else if (Names.TREE_LEVELS.equals(fieldName)) {
                    builder.treeLevels(Integer.parseInt(fieldNode.toString()));
                } else if (Names.DISTANCE_ERROR_PCT.equals(fieldName)) {
                    builder.distanceErrorPct(Double.parseDouble(fieldNode.toString()));
                }
            }
            return builder;
        }
    }

    private final SpatialStrategy spatialStrategy;

    public GeoShapeFieldMapper(FieldMapper.Names names, SpatialPrefixTree prefixTree, double distanceErrorPct,
                               FieldType fieldType, PostingsFormatProvider provider) {
        super(names, 1, fieldType, null, null, provider, null);
        this.spatialStrategy = new TermQueryPrefixTreeStrategy(names, prefixTree, distanceErrorPct);
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType fieldDataType() {
        throw new ElasticSearchIllegalArgumentException("field data on geo_shape field is not supported");
    }

    @Override
    protected Field parseCreateField(ParseContext context) throws IOException {
        return spatialStrategy.createField(GeoJSONShapeParser.parse(context.parser()));
    }

    @Override
    protected void doXContentBody(XContentBuilder builder) throws IOException {
        builder.field("type", contentType());

        // TODO: Come up with a better way to get the name, maybe pass it from builder
        if (spatialStrategy.getPrefixTree() instanceof GeohashPrefixTree) {
            // Don't emit the tree name since GeohashPrefixTree is the default
            // Only emit the tree levels if it isn't the default value
            if (spatialStrategy.getPrefixTree().getMaxLevels() != Defaults.GEOHASH_LEVELS) {
                builder.field(Names.TREE_LEVELS, spatialStrategy.getPrefixTree().getMaxLevels());
            }
        } else {
            builder.field(Names.TREE, Names.QUADTREE);
            if (spatialStrategy.getPrefixTree().getMaxLevels() != Defaults.QUADTREE_LEVELS) {
                builder.field(Names.TREE_LEVELS, spatialStrategy.getPrefixTree().getMaxLevels());
            }
        }

        if (spatialStrategy.getDistanceErrorPct() != Defaults.DISTANCE_ERROR_PCT) {
            builder.field(Names.DISTANCE_ERROR_PCT, spatialStrategy.getDistanceErrorPct());
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public String value(Object value) {
        throw new UnsupportedOperationException("GeoShape fields cannot be converted to String values");
    }

    public SpatialStrategy spatialStrategy() {
        return this.spatialStrategy;
    }
}
