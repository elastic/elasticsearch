/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.index.search.geo;

import com.spatial4j.core.shape.Shape;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredDocIdSet;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.geo.ShapeBuilder;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.strings.StringFieldData;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;

import java.io.IOException;

/**
 * Wraps a GeoShape {@link Filter} that may produce false positive matches, to
 * limit the docs returned DocIdSet to only include docs that actually match the
 * geo shape query.  This is accomplished by doing a JTS relation between the doc's
 * serialized WKB field and the query shape.
 *
 * The Filter that this class wraps must produce at least all docs that match; it
 * should not wrongly omit a matching doc from its DocIdSet.
 */
public class VerifiedShapeRelationFilter extends Filter {
    private Filter baseFilter;
    private FieldDataCache fieldDataCache;
    private String fieldName;
    private ShapeRelation shapeRelation;
    private Geometry geometry;

    /**
     * Instantiates a Filter that accepts documents where both the following are true:
     * <ul><li>baseFilter accepts the doc;</li>
     *     <li>(the doc's Geometry at <tt>fieldName<tt>).shapeRelation(shape) returns true.</li>
     * </ul>
     */
    public VerifiedShapeRelationFilter(Filter baseFilter, GeoShapeFieldMapper fieldMapper, FieldDataCache fieldDataCache, String fieldName, ShapeRelation relation, Shape shape) {
        if (!fieldMapper.isStoreWkb()) {
            throw new ElasticSearchIllegalArgumentException("wkb is not enabled (indexed) for field [" + fieldMapper.name() + "], can't use non-fuzzy matching on it");
        }
        this.baseFilter = baseFilter;
        this.fieldDataCache = fieldDataCache;
        this.fieldName = fieldName;
        this.shapeRelation = relation;
        this.geometry = ShapeBuilder.toJTSGeometry(shape);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits bits) throws IOException {
        final StringFieldData fieldData = (StringFieldData)fieldDataCache.cache(FieldDataType.DefaultTypes.STRING, context.reader(), fieldName);
        DocIdSet candidates = baseFilter.getDocIdSet(context, bits);
        return new VerifiedShapeRelationDocIdSet(candidates, fieldData, shapeRelation, geometry);
    }

    private static class VerifiedShapeRelationDocIdSet extends FilteredDocIdSet {
        // FIXME: fieldData should be binary, once I can figure out how to get
        // the FieldCache to read stored (non-indexed) fields.
        private StringFieldData fieldData;
        private ShapeRelation shapeRelation;
        private Geometry queryGeometry;

        // Uses default GeometryFactory with double floating point precision.
        private WKBReader deserializer = new WKBReader();

        private VerifiedShapeRelationDocIdSet(DocIdSet innerCandidates, FieldData fieldData,
                                              ShapeRelation shapeRelation, Geometry geometry) {
            // This is the assumption that candidates might return false positives,
            // but will never falsely miss a matching document.
            super(innerCandidates);
            this.fieldData = (StringFieldData) fieldData;
            this.shapeRelation = shapeRelation;
            this.queryGeometry = geometry;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean match(int docId) {
            String value = fieldData.docFieldData(docId).getValue();
            Geometry docGeom = null;
            try {
                docGeom = deserializer.read(Base64.decode(value));
            } catch (ParseException parseEx) {
                throw new ElasticSearchParseException("Problem reading document Geometry from cache.", parseEx);
            }
            switch (shapeRelation) {
                case DISJOINT:
                    return docGeom.disjoint(queryGeometry);
                case INTERSECTS:
                    return docGeom.intersects(queryGeometry);
                case WITHIN:
                    return docGeom.within(queryGeometry);
                default:
                    throw new UnsupportedOperationException("Shape Relation [" + shapeRelation.getRelationName() + "] not currently supported");
            }
        }
    }
}
