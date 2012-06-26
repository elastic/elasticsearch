package org.elasticsearch.common.lucene.spatial;

import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.spatial.prefix.NodeTokenStream;
import org.elasticsearch.common.lucene.spatial.prefix.tree.Node;
import org.elasticsearch.common.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.mapper.FieldMapper;

import java.util.List;

/**
 * Abstraction of the logic used to index and filter Shapes.
 */
public abstract class SpatialStrategy {

    private final FieldMapper.Names fieldName;
    private final double distanceErrorPct;
    private final SpatialPrefixTree prefixTree;

    private ThreadLocal<NodeTokenStream> nodeTokenStream = new ThreadLocal<NodeTokenStream>() {

        @Override
        protected NodeTokenStream initialValue() {
            return new NodeTokenStream();
        }
    };

    /**
     * Creates a new SpatialStrategy that will index and Filter using the
     * given field
     *
     * @param fieldName Name of the field that the Strategy will index in and Filter
     * @param prefixTree SpatialPrefixTree that will be used to represent Shapes
     * @param distanceErrorPct Distance Error Percentage used to guide the
     *        SpatialPrefixTree on how precise it should be
     */
    protected SpatialStrategy(FieldMapper.Names fieldName, SpatialPrefixTree prefixTree, double distanceErrorPct) {
        this.fieldName = fieldName;
        this.prefixTree = prefixTree;
        this.distanceErrorPct = distanceErrorPct;
    }

    /**
     * Converts the given Shape into its indexable format.  Implementations
     * should not store the Shape value as well.
     *
     * @param shape Shape to convert ints its indexable format
     * @return Fieldable for indexing the Shape
     */
    public Fieldable createField(Shape shape) {
        int detailLevel = prefixTree.getMaxLevelForPrecision(shape, distanceErrorPct);
        List<Node> nodes = prefixTree.getNodes(shape, detailLevel, true);
        NodeTokenStream tokenStream = nodeTokenStream.get();
        tokenStream.setNodes(nodes);
        return new Field(fieldName.indexName(), tokenStream);
    }

    /**
     * Creates a Filter that will find all indexed Shapes that relate to the
     * given Shape
     *
     * @param shape Shape the indexed shapes will relate to
     * @param relation Nature of the relation
     * @return Filter for finding the related shapes
     */
    public Filter createFilter(Shape shape, ShapeRelation relation) {
        switch (relation) {
            case INTERSECTS:
                return createIntersectsFilter(shape);
            case CONTAINS:
                return createContainsFilter(shape);
            case DISJOINT:
                return createDisjointFilter(shape);
            default:
                throw new UnsupportedOperationException("Shape Relation [" + relation.getRelationName() + "] not currently supported");
        }
    }

    /**
     * Creates a Query that will find all indexed Shapes that relate to the
     * given Shape
     *
     * @param shape Shape the indexed shapes will relate to
     * @param relation Nature of the relation
     * @return Query for finding the related shapes
     */
    public Query createQuery(Shape shape, ShapeRelation relation) {
        switch (relation) {
            case INTERSECTS:
                return createIntersectsQuery(shape);
            case CONTAINS:
                return createContainsQuery(shape);
            case DISJOINT:
                return createDisjointQuery(shape);
            default:
                throw new UnsupportedOperationException("Shape Relation [" + relation.getRelationName() + "] not currently supported");
        }
    }

    /**
     * Creates a Filter that will find all indexed Shapes that intersect with
     * the given Shape
     *
     * @param shape Shape to find the intersection Shapes of
     * @return Filter finding the intersecting indexed Shapes
     */
    public abstract Filter createIntersectsFilter(Shape shape);

    /**
     * Creates a Query that will find all indexed Shapes that intersect with
     * the given Shape
     *
     * @param shape Shape to find the intersection Shapes of
     * @return Query finding the intersecting indexed Shapes
     */
    public abstract Query createIntersectsQuery(Shape shape);

    /**
     * Creates a Filter that will find all indexed Shapes that are disjoint
     * to the given Shape
     *
     * @param shape Shape to find the disjoint Shapes of
     * @return Filter for finding the disjoint indexed Shapes
     */
    public abstract Filter createDisjointFilter(Shape shape);

    /**
     * Creates a Query that will find all indexed Shapes that are disjoint
     * to the given Shape
     *
     * @param shape Shape to find the disjoint Shapes of
     * @return Query for finding the disjoint indexed Shapes
     */
    public abstract Query createDisjointQuery(Shape shape);

    /**
     * Creates a Filter that will find all indexed Shapes that are properly
     * contained within the given Shape (the indexed Shapes will not have
     * any area outside of the given Shape).
     *
     * @param shape Shape to find the contained Shapes of
     * @return Filter for finding the contained indexed Shapes
     */
    public abstract Filter createContainsFilter(Shape shape);

    /**
     * Creates a Query that will find all indexed Shapes that are properly
     * contained within the given Shape (the indexed Shapes will not have
     * any area outside of the given Shape).
     *
     * @param shape Shape to find the contained Shapes of
     * @return Query for finding the contained indexed Shapes
     */
    public abstract Query createContainsQuery(Shape shape);

    /**
     * Returns the name of the field this Strategy applies to
     *
     * @return Name of the field the Strategy applies to
     */
    public FieldMapper.Names getFieldName() {
        return fieldName;
    }

    /**
     * Returns the distance error percentage for this Strategy
     *
     * @return Distance error percentage for the Strategy
     */
    public double getDistanceErrorPct() {
        return distanceErrorPct;
    }

    /**
     * Returns the {@link SpatialPrefixTree} used by this Strategy
     *
     * @return SpatialPrefixTree used by the Strategy
     */
    public SpatialPrefixTree getPrefixTree() {
        return prefixTree;
    }
}
