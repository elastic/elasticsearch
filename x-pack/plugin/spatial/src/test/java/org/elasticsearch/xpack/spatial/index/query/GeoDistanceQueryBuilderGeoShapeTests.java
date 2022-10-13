/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.geo.GeoDistanceQueryBuilderTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@SuppressWarnings("checkstyle:MissingJavadocMethod")
public class GeoDistanceQueryBuilderGeoShapeTests extends GeoDistanceQueryBuilderTestCase {

    private static final String GEO_SHAPE_FIELD_NAME = "mapped_geo_shape";
    protected static final String GEO_SHAPE_ALIAS_FIELD_NAME = "mapped_geo_shape_alias";

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        final XContentBuilder builder = PutMappingRequest.simpleMapping(
            GEO_SHAPE_FIELD_NAME,
            "type=geo_shape",
            GEO_SHAPE_ALIAS_FIELD_NAME,
            "type=alias,path=" + GEO_SHAPE_FIELD_NAME
        );
        mapperService.merge("_doc", new CompressedXContent(Strings.toString(builder)), MapperService.MergeReason.MAPPING_UPDATE);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateSpatialPlugin.class);
    }

    @Override
    protected String getFieldName() {
        return randomFrom(GEO_SHAPE_FIELD_NAME, GEO_SHAPE_ALIAS_FIELD_NAME);
    }

    @Override
    protected void doAssertLuceneQuery(GeoDistanceQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        final MappedFieldType fieldType = context.getFieldType(queryBuilder.fieldName());
        if (fieldType == null) {
            assertTrue("Found no indexed geo query.", query instanceof MatchNoDocsQuery);
        }
        assertEquals(GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType.class, fieldType.getClass());
        if (fieldType.hasDocValues()) {
            assertEquals(IndexOrDocValuesQuery.class, query.getClass());
        } else {
            assertNotEquals(IndexOrDocValuesQuery.class, query.getClass());
        }
    }
}
