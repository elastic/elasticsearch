/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Objects;

/**
 * {@link QueryBuilder} that builds a GeoShape Query
 */
public class GeoShapeQueryBuilder extends AbstractQueryBuilder<GeoShapeQueryBuilder> {

    public static final String NAME = "geo_shape";
    public static final String DEFAULT_SHAPE_INDEX_NAME = "shapes";
    public static final String DEFAULT_SHAPE_FIELD_NAME = "shape";
    public static final ShapeRelation DEFAULT_SHAPE_RELATION = ShapeRelation.INTERSECTS;

    static final GeoShapeQueryBuilder PROTOTYPE = new GeoShapeQueryBuilder("field", new PointBuilder());

    private final String fieldName;

    private ShapeBuilder shape;

    private SpatialStrategy strategy;

    private final String indexedShapeId;
    private final String indexedShapeType;

    private String indexedShapeIndex = DEFAULT_SHAPE_INDEX_NAME;
    private String indexedShapePath = DEFAULT_SHAPE_FIELD_NAME;

    private ShapeRelation relation = DEFAULT_SHAPE_RELATION;

    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
     * field name using the given Shape
     *
     * @param fieldName
     *            Name of the field that will be queried
     * @param shape
     *            Shape used in the Query
     */
    public GeoShapeQueryBuilder(String fieldName, ShapeBuilder shape) {
        this(fieldName, shape, null, null);
    }

    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
     * field name and will use the Shape found with the given ID in the given
     * type
     *
     * @param fieldName
     *            Name of the field that will be filtered
     * @param indexedShapeId
     *            ID of the indexed Shape that will be used in the Query
     * @param indexedShapeType
     *            Index type of the indexed Shapes
     */
    public GeoShapeQueryBuilder(String fieldName, String indexedShapeId, String indexedShapeType) {
        this(fieldName, (ShapeBuilder) null, indexedShapeId, indexedShapeType);
    }

    private GeoShapeQueryBuilder(String fieldName, ShapeBuilder shape, String indexedShapeId, String indexedShapeType) {
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName is required");
        }
        if (shape == null && indexedShapeId == null) {
            throw new IllegalArgumentException("either shapeBytes or indexedShapeId and indexedShapeType are required");
        }
        if (indexedShapeId != null && indexedShapeType == null) {
            throw new IllegalArgumentException("indexedShapeType is required if indexedShapeId is specified");
        }
        this.fieldName = fieldName;
        this.shape = shape;
        this.indexedShapeId = indexedShapeId;
        this.indexedShapeType = indexedShapeType;
    }

    /**
     * @return the name of the field that will be queried
     */
    public String fieldName() {
        return fieldName;
    }

    /**
     * @return the shape used in the Query
     */
    public ShapeBuilder shape() {
        return shape;
    }

    /**
     * @return the ID of the indexed Shape that will be used in the Query
     */
    public String indexedShapeId() {
        return indexedShapeId;
    }

    /**
     * @return the document type of the indexed Shape that will be used in the
     *         Query
     */
    public String indexedShapeType() {
        return indexedShapeType;
    }

    /**
     * Defines which spatial strategy will be used for building the geo shape
     * Query. When not set, the strategy that will be used will be the one that
     * is associated with the geo shape field in the mappings.
     *
     * @param strategy
     *            The spatial strategy to use for building the geo shape Query
     * @return this
     */
    public GeoShapeQueryBuilder strategy(SpatialStrategy strategy) {
        if (strategy != null && strategy == SpatialStrategy.TERM && relation != ShapeRelation.INTERSECTS) {
            throw new IllegalArgumentException("strategy [" + strategy.getStrategyName() + "] only supports relation ["
                    + ShapeRelation.INTERSECTS.getRelationName() + "] found relation [" + relation.getRelationName() + "]");
        }
        this.strategy = strategy;
        return this;
    }

    /**
     * @return The spatial strategy to use for building the geo shape Query
     */
    public SpatialStrategy strategy() {
        return strategy;
    }

    /**
     * Sets the name of the index where the indexed Shape can be found
     *
     * @param indexedShapeIndex Name of the index where the indexed Shape is
     * @return this
     */
    public GeoShapeQueryBuilder indexedShapeIndex(String indexedShapeIndex) {
        this.indexedShapeIndex = indexedShapeIndex;
        return this;
    }

    /**
     * @return the index name for the indexed Shape that will be used in the
     *         Query
     */
    public String indexedShapeIndex() {
        return indexedShapeIndex;
    }

    /**
     * Sets the path of the field in the indexed Shape document that has the Shape itself
     *
     * @param indexedShapePath Path of the field where the Shape itself is defined
     * @return this
     */
    public GeoShapeQueryBuilder indexedShapePath(String indexedShapePath) {
        this.indexedShapePath = indexedShapePath;
        return this;
    }

    /**
     * @return the path of the indexed Shape that will be used in the Query
     */
    public String indexedShapePath() {
        return indexedShapePath;
    }

    /**
     * Sets the relation of query shape and indexed shape.
     *
     * @param relation relation of the shapes
     * @return this
     */
    public GeoShapeQueryBuilder relation(ShapeRelation relation) {
        if (relation == null) {
            throw new IllegalArgumentException("No Shape Relation defined");
        }
        if (strategy != null && strategy == SpatialStrategy.TERM && relation != ShapeRelation.INTERSECTS) {
            throw new IllegalArgumentException("current strategy [" + strategy.getStrategyName() + "] only supports relation ["
                    + ShapeRelation.INTERSECTS.getRelationName() + "] found relation [" + relation.getRelationName() + "]");
        }
        this.relation = relation;
        return this;
    }

    /**
     * @return the relation of query shape and indexed shape to use in the Query
     */
    public ShapeRelation relation() {
        return relation;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        ShapeBuilder shapeToQuery = shape;
        if (shapeToQuery == null) {
            GetRequest getRequest = new GetRequest(indexedShapeIndex, indexedShapeType, indexedShapeId);
            getRequest.copyContextAndHeadersFrom(SearchContext.current());
            shapeToQuery = fetch(context.getClient(), getRequest, indexedShapePath);
        }
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new QueryShardException(context, "Failed to find geo_shape field [" + fieldName + "]");
        }

        // TODO: This isn't the nicest way to check this
        if (!(fieldType instanceof GeoShapeFieldMapper.GeoShapeFieldType)) {
            throw new QueryShardException(context, "Field [" + fieldName + "] is not a geo_shape");
        }

        GeoShapeFieldMapper.GeoShapeFieldType shapeFieldType = (GeoShapeFieldMapper.GeoShapeFieldType) fieldType;

        PrefixTreeStrategy strategy = shapeFieldType.defaultStrategy();
        if (this.strategy != null) {
            strategy = shapeFieldType.resolveStrategy(this.strategy);
        }
        Query query;
        if (strategy instanceof RecursivePrefixTreeStrategy && relation == ShapeRelation.DISJOINT) {
            // this strategy doesn't support disjoint anymore: but it did
            // before, including creating lucene fieldcache (!)
            // in this case, execute disjoint as exists && !intersects
            BooleanQuery.Builder bool = new BooleanQuery.Builder();
            Query exists = ExistsQueryBuilder.newFilter(context, fieldName);
            Query intersects = strategy.makeQuery(getArgs(shapeToQuery, ShapeRelation.INTERSECTS));
            bool.add(exists, BooleanClause.Occur.MUST);
            bool.add(intersects, BooleanClause.Occur.MUST_NOT);
            query = new ConstantScoreQuery(bool.build());
        } else {
            query = new ConstantScoreQuery(strategy.makeQuery(getArgs(shapeToQuery, relation)));
        }
        return query;
    }

    /**
     * Fetches the Shape with the given ID in the given type and index.
     *
     * @param getRequest
     *            GetRequest containing index, type and id
     * @param path
     *            Name or path of the field in the Shape Document where the
     *            Shape itself is located
     * @return Shape with the given ID
     * @throws IOException
     *             Can be thrown while parsing the Shape Document and extracting
     *             the Shape
     */
    private ShapeBuilder fetch(Client client, GetRequest getRequest, String path) throws IOException {
        if (ShapesAvailability.JTS_AVAILABLE == false) {
            throw new IllegalStateException("JTS not available");
        }
        getRequest.preference("_local");
        getRequest.operationThreaded(false);
        GetResponse response = client.get(getRequest).actionGet();
        if (!response.isExists()) {
            throw new IllegalArgumentException("Shape with ID [" + getRequest.id() + "] in type [" + getRequest.type() + "] not found");
        }

        String[] pathElements = Strings.splitStringToArray(path, '.');
        int currentPathSlot = 0;

        XContentParser parser = null;
        try {
            parser = XContentHelper.createParser(response.getSourceAsBytesRef());
            XContentParser.Token currentToken;
            while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (currentToken == XContentParser.Token.FIELD_NAME) {
                    if (pathElements[currentPathSlot].equals(parser.currentName())) {
                        parser.nextToken();
                        if (++currentPathSlot == pathElements.length) {
                            return ShapeBuilder.parse(parser);
                        }
                    } else {
                        parser.nextToken();
                        parser.skipChildren();
                    }
                }
            }
            throw new IllegalStateException("Shape with name [" + getRequest.id() + "] found but missing " + path + " field");
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public static SpatialArgs getArgs(ShapeBuilder shape, ShapeRelation relation) {
        switch (relation) {
        case DISJOINT:
            return new SpatialArgs(SpatialOperation.IsDisjointTo, shape.build());
        case INTERSECTS:
            return new SpatialArgs(SpatialOperation.Intersects, shape.build());
        case WITHIN:
            return new SpatialArgs(SpatialOperation.IsWithin, shape.build());
        case CONTAINS:
            return new SpatialArgs(SpatialOperation.Contains, shape.build());
        default:
            throw new IllegalArgumentException("invalid relation [" + relation + "]");
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.startObject(fieldName);

        if (strategy != null) {
            builder.field(GeoShapeQueryParser.STRATEGY_FIELD.getPreferredName(), strategy.getStrategyName());
        }

        if (shape != null) {
            builder.field(GeoShapeQueryParser.SHAPE_FIELD.getPreferredName());
            shape.toXContent(builder, params);
        } else {
            builder.startObject(GeoShapeQueryParser.INDEXED_SHAPE_FIELD.getPreferredName())
                    .field(GeoShapeQueryParser.SHAPE_ID_FIELD.getPreferredName(), indexedShapeId)
                    .field(GeoShapeQueryParser.SHAPE_TYPE_FIELD.getPreferredName(), indexedShapeType);
            if (indexedShapeIndex != null) {
                builder.field(GeoShapeQueryParser.SHAPE_INDEX_FIELD.getPreferredName(), indexedShapeIndex);
            }
            if (indexedShapePath != null) {
                builder.field(GeoShapeQueryParser.SHAPE_PATH_FIELD.getPreferredName(), indexedShapePath);
            }
            builder.endObject();
        }

        if(relation != null) {
            builder.field(GeoShapeQueryParser.RELATION_FIELD.getPreferredName(), relation.getRelationName());
        }

        builder.endObject();

        printBoostAndQueryName(builder);

        builder.endObject();
    }

    @Override
    protected GeoShapeQueryBuilder doReadFrom(StreamInput in) throws IOException {
        String fieldName = in.readString();
        GeoShapeQueryBuilder builder;
        if (in.readBoolean()) {
            builder = new GeoShapeQueryBuilder(fieldName, in.readShape());
        } else {
            String indexedShapeId = in.readOptionalString();
            String indexedShapeType = in.readOptionalString();
            String indexedShapeIndex = in.readOptionalString();
            String indexedShapePath = in.readOptionalString();
            builder = new GeoShapeQueryBuilder(fieldName, indexedShapeId, indexedShapeType);
            if (indexedShapeIndex != null) {
                builder.indexedShapeIndex = indexedShapeIndex;
            }
            if (indexedShapePath != null) {
                builder.indexedShapePath = indexedShapePath;
            }
        }
        builder.relation = ShapeRelation.DISJOINT.readFrom(in);
        if (in.readBoolean()) {
            builder.strategy = SpatialStrategy.RECURSIVE.readFrom(in);
        }
        return builder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        boolean hasShape = shape != null;
        out.writeBoolean(hasShape);
        if (hasShape) {
            out.writeShape(shape);
        } else {
            out.writeOptionalString(indexedShapeId);
            out.writeOptionalString(indexedShapeType);
            out.writeOptionalString(indexedShapeIndex);
            out.writeOptionalString(indexedShapePath);
        }
        relation.writeTo(out);
        if (strategy == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            strategy.writeTo(out);
        }
    }

    @Override
    protected boolean doEquals(GeoShapeQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
                && Objects.equals(indexedShapeId, other.indexedShapeId)
                && Objects.equals(indexedShapeIndex, other.indexedShapeIndex)
                && Objects.equals(indexedShapePath, other.indexedShapePath)
                && Objects.equals(indexedShapeType, other.indexedShapeType)
                && Objects.equals(relation, other.relation)
                && Objects.equals(shape, other.shape)
                && Objects.equals(strategy, other.strategy);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, indexedShapeId, indexedShapeIndex,
                indexedShapePath, indexedShapeType, relation, shape, strategy);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
