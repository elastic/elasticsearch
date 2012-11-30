package org.elasticsearch.index.mapper.geo;

import com.spatial4j.core.shape.Shape;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import com.vividsolutions.jts.io.WKBWriter;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.geo.GeoJSONShapeParser;
import org.elasticsearch.common.geo.GeoShapeConstants;
import org.elasticsearch.common.geo.ShapeBuilder;
import org.elasticsearch.common.lucene.spatial.SpatialStrategy;
import org.elasticsearch.common.lucene.spatial.prefix.TermQueryPrefixTreeStrategy;
import org.elasticsearch.common.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.elasticsearch.common.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.elasticsearch.common.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ObjectMapperListener;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.Booleans.parseBoolean;
import static org.elasticsearch.common.Strings.toUnderscoreCase;
import static org.elasticsearch.index.mapper.MapperBuilders.stringField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parsePathType;

/**
 * FieldMapper for indexing {@link com.spatial4j.core.shape.Shape}s.
 * <p/>
 * Currently Shapes can only be indexed and can only be queried using
 * {@link org.elasticsearch.index.query.GeoShapeFilterParser}, consequently
 * a lot of behavior in this Mapper is disabled.
 * <p/>
 * Format supported is equivalent to GeoJSON:
 * <p/>
 * "field" : {
 *     "type" : "polygon",
 *     "coordinates" : [
 *         [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 *     ]
 * }
 * <p/>
 * Index format:<br />
 * GeoShapeFieldMapper writes GeoShapeIndexFieldMapper keys directly under its
 * top-level name. When 'wkb' is true, also a StringFieldMapper containing the
 * Shape's base64-encoded WKB serialization under the "wkb" subpath.
 */
public class GeoShapeFieldMapper implements Mapper {

    public static final String CONTENT_TYPE = "geo_shape";

    public static class Names {
        public static final String TREE = "tree";
        public static final String TREE_LEVELS = "tree_levels";
        public static final String GEOHASH = "geohash";
        public static final String QUADTREE = "quadtree";
        public static final String DISTANCE_ERROR_PCT = "distance_error_pct";
        public static final String WKB = "wkb";
    }

    public static class Defaults {
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;

        public static final String TREE = Names.GEOHASH;
        public static final int GEOHASH_LEVELS = GeohashPrefixTree.getMaxLevelsPossible();
        public static final int QUADTREE_LEVELS = QuadPrefixTree.DEFAULT_MAX_LEVELS;
        public static final double DISTANCE_ERROR_PCT = 0.025d;
        public static final boolean STORE_WKB = false;

        public static final FieldType GEO_SHAPE_FIELD_TYPE = new FieldType();

        static {
            GEO_SHAPE_FIELD_TYPE.setIndexed(true);
            GEO_SHAPE_FIELD_TYPE.setTokenized(false);
            GEO_SHAPE_FIELD_TYPE.setStored(false);
            GEO_SHAPE_FIELD_TYPE.setStoreTermVectors(false);
            GEO_SHAPE_FIELD_TYPE.setOmitNorms(true);
            GEO_SHAPE_FIELD_TYPE.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
            GEO_SHAPE_FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends Mapper.Builder<Builder, GeoShapeFieldMapper> {
        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        private boolean storeWkb = Defaults.STORE_WKB;
        private String tree = Defaults.TREE;
        private int treeLevels;
        private double distanceErrorPct = Defaults.DISTANCE_ERROR_PCT;

        public Builder(String name) {
            super(name);
            this.builder = this;
        }

        public Builder pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return this;
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

        public Builder storeWkb(boolean storeWkb) {
            this.storeWkb = storeWkb;
            return this;
        }


        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            SpatialPrefixTree prefixTree;
            if (tree.equals(Names.GEOHASH)) {
                int levels = treeLevels != 0 ? treeLevels : Defaults.GEOHASH_LEVELS;
                prefixTree = new GeohashPrefixTree(GeoShapeConstants.SPATIAL_CONTEXT, levels);
            } else if (tree.equals(Names.QUADTREE)) {
                int levels = treeLevels != 0 ? treeLevels : Defaults.QUADTREE_LEVELS;
                prefixTree = new QuadPrefixTree(GeoShapeConstants.SPATIAL_CONTEXT, levels);
            } else {
                throw new ElasticSearchIllegalArgumentException("Unknown prefix tree type [" + tree + "]");
            }
            GeoShapeIndexFieldMapper spatialTreeMapper =
                    new GeoShapeIndexFieldMapper.Builder(name)
                                .prefixTree(prefixTree)
                                .distanceErrorPct(distanceErrorPct)
                                .build(context);


            context.path().add(name);
            StringFieldMapper wkbMapper = null;
            if (storeWkb) {
                wkbMapper = stringField(Names.WKB)
                                .index(true) // This is required for reading on the other end; how to bypass?
                                .tokenized(false)
                                .includeInAll(false)
                                .omitNorms(true)
                                .indexOptions(FieldInfo.IndexOptions.DOCS_ONLY)
                                .store(true)
                                .build(context);
            }

            context.path().remove();
            context.path().pathType(origPathType);
            return new GeoShapeFieldMapper(name, pathType, storeWkb, spatialTreeMapper, wkbMapper);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, TypeParser.ParserContext parserContext) throws MapperParsingException {
            GeoShapeFieldMapper.Builder builder = new GeoShapeFieldMapper.Builder(name);

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    builder.pathType(parsePathType(name, fieldNode.toString()));
                } else
                if (Names.TREE.equals(fieldName)) {
                    builder.tree(fieldNode.toString());
                } else if (Names.TREE_LEVELS.equals(fieldName)) {
                    builder.treeLevels(Integer.parseInt(fieldNode.toString()));
                } else if (Names.DISTANCE_ERROR_PCT.equals(fieldName)) {
                    builder.distanceErrorPct(Double.parseDouble(fieldNode.toString()));
                } else if (Names.WKB.equals(fieldName)) {
                    builder.storeWkb(parseBoolean(fieldNode.toString(), Defaults.STORE_WKB));
                }
            }
            return builder;
        }
    }

    private final String name;
    private final ContentPath.Type pathType;

    private final GeoShapeIndexFieldMapper spatialTreeMapper;
    private final StringFieldMapper wkbMapper;
    private final boolean storeWkb;

    private GeoShapeFieldMapper(String name, ContentPath.Type pathType, boolean storeWkb,
                                GeoShapeIndexFieldMapper spatialTreeMapper, StringFieldMapper wkbMapper) {
        this.name = name;
        this.pathType = pathType;
        this.storeWkb = storeWkb;
        this.spatialTreeMapper = spatialTreeMapper;
        this.spatialTreeMapper.geoMapper = this;
        this.wkbMapper = wkbMapper;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", CONTENT_TYPE);

        if (pathType != Defaults.PATH_TYPE) {
            builder.field("path", pathType.name().toLowerCase());
        }
        if (storeWkb != Defaults.STORE_WKB) {
            builder.field(Names.WKB, storeWkb);
        }

        // TODO: Come up with a better way to get the name, maybe pass it from builder
        if (spatialStrategy().getPrefixTree() instanceof GeohashPrefixTree) {
            // Don't emit the tree name since GeohashPrefixTree is the default
            // Only emit the tree levels if it isn't the default value
            if (spatialStrategy().getPrefixTree().getMaxLevels() != Defaults.GEOHASH_LEVELS) {
                builder.field(Names.TREE_LEVELS, spatialStrategy().getPrefixTree().getMaxLevels());
            }
        } else {
            builder.field(Names.TREE, Names.QUADTREE);
            if (spatialStrategy().getPrefixTree().getMaxLevels() != Defaults.QUADTREE_LEVELS) {
                builder.field(Names.TREE_LEVELS, spatialStrategy().getPrefixTree().getMaxLevels());
            }
        }

        if (spatialStrategy().getDistanceErrorPct() != Defaults.DISTANCE_ERROR_PCT) {
            builder.field(Names.DISTANCE_ERROR_PCT, spatialStrategy().getDistanceErrorPct());
        }

        builder.endObject();
        return builder;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);
        context.path().add(name);

        Shape shape = GeoJSONShapeParser.parse(context.parser());
        context.externalValue(shape);
        this.spatialTreeMapper.parse(context);

        if (storeWkb) {
            parseWkb(context, shape);
        }

        context.path().remove();
        context.path().pathType(origPathType);
    }

    /**
     * Writes the WKB serialization of the <tt>shape</tt> to the given ParseContext
     * as an external value.
     */
    private void parseWkb(ParseContext context, Shape shape) throws IOException {
        WKBWriter serializer = new WKBWriter(2);
        byte[] wkb = serializer.write(ShapeBuilder.toJTSGeometry(shape));
        context.externalValue(Base64.encode(wkb)); // FIXME: If I can get FieldDataCache to read a non-indexed field, use BinaryFieldMapper.
        this.wkbMapper.parse(context);
    }


    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // TODO (this method is also a TODO in GeoPointFieldMapper, the prototype for this class)
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        spatialTreeMapper.traverse(fieldMapperListener);
        if (storeWkb) {
            wkbMapper.traverse(fieldMapperListener);
        }
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
    }

    @Override
    public void close() {
        spatialTreeMapper.close();
        if (wkbMapper != null) {
            wkbMapper.close();
        }
    }

    public SpatialStrategy spatialStrategy() {
        return spatialTreeMapper.spatialStrategy();
    }

    public static class GeoShapeIndexFieldMapper extends AbstractFieldMapper<String> {

        public static class Builder extends AbstractFieldMapper.Builder<Builder, GeoShapeIndexFieldMapper> {

            private double distanceErrorPct;
            private SpatialPrefixTree prefixTree;

            public Builder(String name) {
                super(name, new FieldType(GeoShapeFieldMapper.Defaults.GEO_SHAPE_FIELD_TYPE));
            }

            public Builder prefixTree(SpatialPrefixTree prefixTree) {
                this.prefixTree = prefixTree;
                return this;
            }

            public Builder distanceErrorPct(double distanceErrorPct) {
                this.distanceErrorPct = distanceErrorPct;
                return this;
            }

            @Override
            public GeoShapeIndexFieldMapper build(BuilderContext context) {
                return new GeoShapeIndexFieldMapper(buildNames(context), prefixTree, distanceErrorPct, fieldType, provider);
            }
        }

        private final SpatialStrategy spatialStrategy;
        private GeoShapeFieldMapper geoMapper;

        public GeoShapeIndexFieldMapper(FieldMapper.Names names, SpatialPrefixTree prefixTree, double distanceErrorPct,
                                   FieldType fieldType, PostingsFormatProvider provider) {
            super(names, 1, fieldType, null, null, provider, null);
            this.spatialStrategy = new TermQueryPrefixTreeStrategy(names, prefixTree, distanceErrorPct);
        }

        @Override
        protected Field parseCreateField(ParseContext context) throws IOException {
            return spatialStrategy.createField((Shape) context.externalValue());
        }

        @Override
        protected String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public String value(Field field) {
            throw new UnsupportedOperationException("GeoShape fields cannot be converted to String values");
        }

        @Override
        public String valueFromString(String value) {
            throw new UnsupportedOperationException("GeoShape fields cannot be converted to String values");
        }

        @Override
        public String valueAsString(Field field) {
            throw new UnsupportedOperationException("GeoShape fields cannot be converted to String values");
        }

        public SpatialStrategy spatialStrategy() {
            return this.spatialStrategy;
        }

        public GeoShapeFieldMapper geoMapper() {
            return this.geoMapper;
        }
    }
}
