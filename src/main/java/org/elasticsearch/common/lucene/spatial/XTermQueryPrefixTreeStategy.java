package org.elasticsearch.common.lucene.spatial;

import java.util.List;

import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.Node;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoShapeConstants;
import org.elasticsearch.common.geo.ShapeBuilder;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapper.Names;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.operation.buffer.BufferOp;
import com.vividsolutions.jts.operation.buffer.BufferParameters;

/**
 * Implementation of {@link SpatialStrategy} that uses TermQuerys / TermFilters
 * to query and filter for Shapes related to other Shapes.
 */
public final class XTermQueryPrefixTreeStategy extends PrefixTreeStrategy {

    private static final double WITHIN_BUFFER_DISTANCE = 0.5;
    private static final BufferParameters BUFFER_PARAMETERS = new BufferParameters(3, BufferParameters.CAP_SQUARE);
    private final Names fieldName;

    /**
     * Creates a new XTermQueryPrefixTreeStategy
     *
     * @param prefixTree       SpatialPrefixTree that will be used to represent Shapes
     * @param fieldName        Name of the field the Strategy applies to
     */
    public XTermQueryPrefixTreeStategy(SpatialPrefixTree prefixTree, FieldMapper.Names fieldName) {
        super(prefixTree, fieldName.indexName());
        this.fieldName = fieldName;
    }
    
    private static double resolveDistErr(Shape shape, SpatialContext ctx, double distErrPct) {
      return SpatialArgs.calcDistanceFromErrPct(shape, distErrPct, ctx);
    }

    public Filter createIntersectsFilter(Shape shape) {
        int detailLevel = getGrid().getLevelForDistance(resolveDistErr(shape, ctx, getDistErrPct()));
        List<Node> nodes = getGrid().getNodes(shape, detailLevel, false);

        BytesRef[] nodeTerms = new BytesRef[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            nodeTerms[i] = new BytesRef(nodes.get(i).getTokenString());
        }
        return new TermsFilter(fieldName.indexName(), nodeTerms);
    }

    public Query createIntersectsQuery(Shape shape) {
        int detailLevel = getGrid().getLevelForDistance(resolveDistErr(shape, ctx, getDistErrPct()));
        List<Node> nodes = getGrid().getNodes(shape, detailLevel, false);

        BooleanQuery query = new BooleanQuery();
        for (Node node : nodes) {
            query.add(new TermQuery(fieldName.createIndexNameTerm(node.getTokenString())),
                    BooleanClause.Occur.SHOULD);
        }

        return new ConstantScoreQuery(query);
    }

    public Filter createDisjointFilter(Shape shape) {
        int detailLevel = getGrid().getLevelForDistance(resolveDistErr(shape, ctx, getDistErrPct()));
        List<Node> nodes = getGrid().getNodes(shape, detailLevel, false);

        XBooleanFilter filter = new XBooleanFilter();
        for (Node node : nodes) {
            filter.add(new TermFilter(fieldName.createIndexNameTerm(node.getTokenString())), BooleanClause.Occur.MUST_NOT);
        }

        return filter;
    }

    public Query createDisjointQuery(Shape shape) {
        int detailLevel = getGrid().getLevelForDistance(resolveDistErr(shape, ctx, getDistErrPct()));
        List<Node> nodes = getGrid().getNodes(shape, detailLevel, false);

        BooleanQuery query = new BooleanQuery();
        query.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        for (Node node : nodes) {
            query.add(new TermQuery(fieldName.createIndexNameTerm(node.getTokenString())),
                    BooleanClause.Occur.MUST_NOT);
        }

        return new ConstantScoreQuery(query);
    }

    public Filter createWithinFilter(Shape shape) {
        Filter intersectsFilter = createIntersectsFilter(shape);

        Geometry shapeGeometry = ShapeBuilder.toJTSGeometry(shape);
        Geometry buffer = BufferOp.bufferOp(shapeGeometry, WITHIN_BUFFER_DISTANCE, BUFFER_PARAMETERS);
        Shape bufferedShape = new JtsGeometry(buffer.difference(shapeGeometry), GeoShapeConstants.SPATIAL_CONTEXT, true);
        Filter bufferedFilter = createIntersectsFilter(bufferedShape);

        XBooleanFilter filter = new XBooleanFilter();
        filter.add(intersectsFilter, BooleanClause.Occur.SHOULD);
        filter.add(bufferedFilter, BooleanClause.Occur.MUST_NOT);

        return filter;
    }

    public Query createWithinQuery(Shape shape) {
        Query intersectsQuery = createIntersectsQuery(shape);

        Geometry shapeGeometry = ShapeBuilder.toJTSGeometry(shape);
        Geometry buffer = BufferOp.bufferOp(shapeGeometry, WITHIN_BUFFER_DISTANCE, BUFFER_PARAMETERS);
        Shape bufferedShape = new JtsGeometry(buffer.difference(shapeGeometry), GeoShapeConstants.SPATIAL_CONTEXT, true);
        Query bufferedQuery = createIntersectsQuery(bufferedShape);

        BooleanQuery query = new BooleanQuery();
        query.add(intersectsQuery, BooleanClause.Occur.SHOULD);
        query.add(bufferedQuery, BooleanClause.Occur.MUST_NOT);

        return new ConstantScoreQuery(query);
    }
    
    @Override
    public  Filter makeFilter(SpatialArgs args) {
        if (args.getOperation() == SpatialOperation.Intersects) {
            return createIntersectsFilter(args.getShape());

        } else if (args.getOperation() == SpatialOperation.IsWithin) {
            return createWithinFilter(args.getShape());

        } else if (args.getOperation() == SpatialOperation.IsDisjointTo) { 
            return createDisjointFilter(args.getShape());

        }
        throw new UnsupportedOperationException("Shape Relation [" + args.getOperation().getName() + "] not currently supported");
    }

    public Query makeQuery(SpatialArgs args) {
        if (args.getOperation() == SpatialOperation.Intersects) {
            return createIntersectsQuery(args.getShape());

        } else if (args.getOperation() == SpatialOperation.IsWithin) {
            return createWithinQuery(args.getShape());

        } else if (args.getOperation() == SpatialOperation.IsDisjointTo) { 
            return createDisjointQuery(args.getShape());

        }
        throw new UnsupportedOperationException("Shape Relation [" + args.getOperation().getName() + "] not currently supported");
    }

}

