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
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * {@link QueryBuilder} that builds a GeoShape Query
 */
public class GeoShapeQueryBuilder extends AbstractQueryBuilder<GeoShapeQueryBuilder> {
    public static final String NAME = "geo_shape";

    public static final String DEFAULT_SHAPE_INDEX_NAME = "shapes";
    public static final String DEFAULT_SHAPE_FIELD_NAME = "shape";
    public static final ShapeRelation DEFAULT_SHAPE_RELATION = ShapeRelation.INTERSECTS;

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField SHAPE_FIELD = new ParseField("shape");
    private static final ParseField STRATEGY_FIELD = new ParseField("strategy");
    private static final ParseField RELATION_FIELD = new ParseField("relation");
    private static final ParseField INDEXED_SHAPE_FIELD = new ParseField("indexed_shape");
    private static final ParseField SHAPE_ID_FIELD = new ParseField("id");
    private static final ParseField SHAPE_TYPE_FIELD = new ParseField("type");
    private static final ParseField SHAPE_INDEX_FIELD = new ParseField("index");
    private static final ParseField SHAPE_PATH_FIELD = new ParseField("path");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final String fieldName;

    private final ShapeBuilder shape;
    private final Supplier<ShapeBuilder> supplier;

    private SpatialStrategy strategy;

    private final String indexedShapeId;
    private final String indexedShapeType;

    private String indexedShapeIndex = DEFAULT_SHAPE_INDEX_NAME;
    private String indexedShapePath = DEFAULT_SHAPE_FIELD_NAME;

    private ShapeRelation relation = DEFAULT_SHAPE_RELATION;

    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

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
        this.supplier = null;
    }

    private GeoShapeQueryBuilder(String fieldName, Supplier<ShapeBuilder> supplier, String indexedShapeId, String indexedShapeType) {
        this.fieldName = fieldName;
        this.shape = null;
        this.supplier = supplier;
        this.indexedShapeId = indexedShapeId;
        this.indexedShapeType = indexedShapeType;
    }

    /**
     * Read from a stream.
     */
    public GeoShapeQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        if (in.readBoolean()) {
            shape = in.readNamedWriteable(ShapeBuilder.class);
            indexedShapeId = null;
            indexedShapeType = null;
        } else {
            shape = null;
            indexedShapeId = in.readOptionalString();
            indexedShapeType = in.readOptionalString();
            indexedShapeIndex = in.readOptionalString();
            indexedShapePath = in.readOptionalString();
        }
        relation = ShapeRelation.readFromStream(in);
        strategy = in.readOptionalWriteable(SpatialStrategy::readFromStream);
        ignoreUnmapped = in.readBoolean();
        supplier = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (supplier != null) {
            throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        boolean hasShape = shape != null;
        out.writeBoolean(hasShape);
        if (hasShape) {
            out.writeNamedWriteable(shape);
        } else {
            out.writeOptionalString(indexedShapeId);
            out.writeOptionalString(indexedShapeType);
            out.writeOptionalString(indexedShapeIndex);
            out.writeOptionalString(indexedShapePath);
        }
        relation.writeTo(out);
        out.writeOptionalWriteable(strategy);
        out.writeBoolean(ignoreUnmapped);
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

    /**
     * Sets whether the query builder should ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public GeoShapeQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        return this;
    }

    /**
     * Gets whether the query builder will ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        if (shape == null || supplier != null) {
            throw new UnsupportedOperationException("query must be rewritten first");
        }
        final ShapeBuilder shapeToQuery = shape;
        final MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "failed to find geo_shape field [" + fieldName + "]");
            }
        }

        // TODO: This isn't the nicest way to check this
        if (!(fieldType instanceof GeoShapeFieldMapper.GeoShapeFieldType)) {
            throw new QueryShardException(context, "Field [" + fieldName + "] is not a geo_shape");
        }

        final GeoShapeFieldMapper.GeoShapeFieldType shapeFieldType = (GeoShapeFieldMapper.GeoShapeFieldType) fieldType;

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
     */
    private void fetch(Client client, GetRequest getRequest, String path, ActionListener<ShapeBuilder> listener) {
        if (ShapesAvailability.JTS_AVAILABLE == false) {
            throw new IllegalStateException("JTS not available");
        }
        getRequest.preference("_local");
        getRequest.operationThreaded(false);
        client.get(getRequest, new ActionListener<GetResponse>(){

            @Override
            public void onResponse(GetResponse response) {
                try {
                    if (!response.isExists()) {
                        throw new IllegalArgumentException("Shape with ID [" + getRequest.id() + "] in type [" + getRequest.type()
                            + "] not found");
                    }
                    if (response.isSourceEmpty()) {
                        throw new IllegalArgumentException("Shape with ID [" + getRequest.id() + "] in type [" + getRequest.type() +
                            "] source disabled");
                    }

                    String[] pathElements = path.split("\\.");
                    int currentPathSlot = 0;

                    // It is safe to use EMPTY here because this never uses namedObject
                    try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, response.getSourceAsBytesRef())) {
                        XContentParser.Token currentToken;
                        while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (currentToken == XContentParser.Token.FIELD_NAME) {
                                if (pathElements[currentPathSlot].equals(parser.currentName())) {
                                    parser.nextToken();
                                    if (++currentPathSlot == pathElements.length) {
                                        listener.onResponse(ShapeBuilder.parse(parser));
                                    }
                                } else {
                                    parser.nextToken();
                                    parser.skipChildren();
                                }
                            }
                        }
                        throw new IllegalStateException("Shape with name [" + getRequest.id() + "] found but missing " + path + " field");
                    }
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });

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
            builder.field(STRATEGY_FIELD.getPreferredName(), strategy.getStrategyName());
        }

        if (shape != null) {
            builder.field(SHAPE_FIELD.getPreferredName());
            shape.toXContent(builder, params);
        } else {
            builder.startObject(INDEXED_SHAPE_FIELD.getPreferredName())
                    .field(SHAPE_ID_FIELD.getPreferredName(), indexedShapeId)
                    .field(SHAPE_TYPE_FIELD.getPreferredName(), indexedShapeType);
            if (indexedShapeIndex != null) {
                builder.field(SHAPE_INDEX_FIELD.getPreferredName(), indexedShapeIndex);
            }
            if (indexedShapePath != null) {
                builder.field(SHAPE_PATH_FIELD.getPreferredName(), indexedShapePath);
            }
            builder.endObject();
        }

        if(relation != null) {
            builder.field(RELATION_FIELD.getPreferredName(), relation.getRelationName());
        }

        builder.endObject();
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);

        printBoostAndQueryName(builder);

        builder.endObject();
    }

    public static GeoShapeQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        ShapeRelation shapeRelation = null;
        SpatialStrategy strategy = null;
        ShapeBuilder shape = null;

        String id = null;
        String type = null;
        String index = null;
        String shapePath = null;

        XContentParser.Token token;
        String currentFieldName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (fieldName != null) {
                    throw new ParsingException(parser.getTokenLocation(), "[" +
                            GeoShapeQueryBuilder.NAME + "] point specified twice. [" + currentFieldName + "]");
                }
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (SHAPE_FIELD.match(currentFieldName)) {
                            shape = ShapeBuilder.parse(parser);
                        } else if (STRATEGY_FIELD.match(currentFieldName)) {
                            String strategyName = parser.text();
                            strategy = SpatialStrategy.fromString(strategyName);
                            if (strategy == null) {
                                throw new ParsingException(parser.getTokenLocation(), "Unknown strategy [" + strategyName + " ]");
                            }
                        } else if (RELATION_FIELD.match(currentFieldName)) {
                            shapeRelation = ShapeRelation.getRelationByName(parser.text());
                            if (shapeRelation == null) {
                                throw new ParsingException(parser.getTokenLocation(), "Unknown shape operation [" + parser.text() + " ]");
                            }
                        } else if (INDEXED_SHAPE_FIELD.match(currentFieldName)) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if (SHAPE_ID_FIELD.match(currentFieldName)) {
                                        id = parser.text();
                                    } else if (SHAPE_TYPE_FIELD.match(currentFieldName)) {
                                        type = parser.text();
                                    } else if (SHAPE_INDEX_FIELD.match(currentFieldName)) {
                                        index = parser.text();
                                    } else if (SHAPE_PATH_FIELD.match(currentFieldName)) {
                                        shapePath = parser.text();
                                    }
                                } else {
                                    throw new ParsingException(parser.getTokenLocation(), "[" + GeoShapeQueryBuilder.NAME +
                                            "] unknown token [" + token + "] after [" + currentFieldName + "]");
                                }
                            }
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[" + GeoShapeQueryBuilder.NAME +
                                    "] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
                    queryName = parser.text();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName)) {
                    ignoreUnmapped = parser.booleanValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + GeoShapeQueryBuilder.NAME +
                            "] query does not support [" + currentFieldName + "]");
                }
            }
        }
        GeoShapeQueryBuilder builder;
        if (shape != null) {
            builder = new GeoShapeQueryBuilder(fieldName, shape);
        } else {
            builder = new GeoShapeQueryBuilder(fieldName, id, type);
        }
        if (index != null) {
            builder.indexedShapeIndex(index);
        }
        if (shapePath != null) {
            builder.indexedShapePath(shapePath);
        }
        if (shapeRelation != null) {
            builder.relation(shapeRelation);
        }
        if (strategy != null) {
            builder.strategy(strategy);
        }
        if (queryName != null) {
            builder.queryName(queryName);
        }
        builder.boost(boost);
        builder.ignoreUnmapped(ignoreUnmapped);
        return builder;
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
                && Objects.equals(supplier, other.supplier)
                && Objects.equals(strategy, other.strategy)
                && Objects.equals(ignoreUnmapped, other.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, indexedShapeId, indexedShapeIndex,
                indexedShapePath, indexedShapeType, relation, shape, strategy, ignoreUnmapped, supplier);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (supplier != null) {
            return supplier.get() == null ? this : new GeoShapeQueryBuilder(this.fieldName, supplier.get()).relation(relation).strategy
                (strategy);
        } else if (this.shape == null) {
            SetOnce<ShapeBuilder> supplier = new SetOnce<>();
            queryRewriteContext.registerAsyncAction((client, listener) -> {
                GetRequest getRequest = new GetRequest(indexedShapeIndex, indexedShapeType, indexedShapeId);
                fetch(client, getRequest, indexedShapePath, ActionListener.wrap(builder-> {
                    supplier.set(builder);
                    listener.onResponse(null);
                }, listener::onFailure));
            });
            return new GeoShapeQueryBuilder(this.fieldName, supplier::get, this.indexedShapeId, this.indexedShapeType).relation(relation)
                .strategy(strategy);
        }
        return this;
    }
}
