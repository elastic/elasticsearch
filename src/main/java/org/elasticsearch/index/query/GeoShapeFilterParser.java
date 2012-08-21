package org.elasticsearch.index.query;

import com.spatial4j.core.shape.Shape;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.GeoJSONShapeParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameFilter;

/**
 * {@link FilterParser} for filtering Documents based on {@link Shape}s.
 *
 * Only those fields mapped using {@link GeoShapeFieldMapper} can be filtered
 * using this parser.
 *
 * Format supported:
 *
 * "field" : {
 *     "relation" : "intersects",
 *     "shape" : {
 *         "type" : "polygon",
 *         "coordinates" : [
 *              [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 *         ]
 *     }
 * }
 */
public class GeoShapeFilterParser implements FilterParser {

    public static final String NAME = "geo_shape";

    @Override
    public String[] names() {
        return new String[]{NAME, "geoShape"};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;
        ShapeRelation shapeRelation = null;
        Shape shape = null;
        boolean cache = false;
        CacheKeyFilter.Key cacheKey = null;
        String filterName = null;

        XContentParser.Token token;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();

                        token = parser.nextToken();
                        if ("shape".equals(currentFieldName)) {
                            shape = GeoJSONShapeParser.parse(parser);
                        } else if ("relation".equals(currentFieldName)) {
                            shapeRelation = ShapeRelation.getRelationByName(parser.text());
                            if (shapeRelation == null) {
                                throw new QueryParsingException(parseContext.index(), "Unknown shape operation [" + parser.text() + " ]");
                            }
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                } else if ("_cache_key".equals(currentFieldName)) {
                    cacheKey = new CacheKeyFilter.Key(parser.text());
                }
            }
        }

        if (shape == null) {
            throw new QueryParsingException(parseContext.index(), "No Shape defined");
        } else if (shapeRelation == null) {
            throw new QueryParsingException(parseContext.index(), "No Shape Relation defined");
        }

        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers == null || !smartNameFieldMappers.hasMapper()) {
            throw new QueryParsingException(parseContext.index(), "Failed to find geo_shape field [" + fieldName + "]");
        }

        FieldMapper fieldMapper = smartNameFieldMappers.mapper();
        // TODO: This isn't the nicest way to check this
        if (!(fieldMapper instanceof GeoShapeFieldMapper)) {
            throw new QueryParsingException(parseContext.index(), "Field [" + fieldName + "] is not a geo_shape");
        }

        GeoShapeFieldMapper shapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        Filter filter = shapeFieldMapper.spatialStrategy().createFilter(shape, shapeRelation);

        if (cache) {
            filter = parseContext.cacheFilter(filter, cacheKey);
        }

        filter = wrapSmartNameFilter(filter, smartNameFieldMappers, parseContext);

        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }

        return filter;
    }
}
