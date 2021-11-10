/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class GeoShapeWithDocValuesQueryBuilderTests extends AbstractQueryTestCase<GeoShapeQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateSpatialPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {

        if (mapperService.parserContext().indexVersionCreated().before(Version.V_6_6_0) || randomBoolean()) {
            XContentBuilder mapping = jsonBuilder().startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("test")
                .field("type", "geo_shape")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
            mapperService.merge("_doc", new CompressedXContent(Strings.toString(mapping)), MapperService.MergeReason.MAPPING_UPDATE);
        } else {
            XContentBuilder mapping = jsonBuilder().startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("test")
                .field("type", "geo_shape")
                .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject()
                .endObject();
            mapperService.merge("_doc", new CompressedXContent(Strings.toString(mapping)), MapperService.MergeReason.MAPPING_UPDATE);
        }
    }

    @Override
    protected GeoShapeQueryBuilder doCreateTestQueryBuilder() {
        Geometry geometry = randomFrom(
            GeometryTestUtils.randomPoint(false),
            GeometryTestUtils.randomLine(false),
            GeometryTestUtils.randomPolygon(false)
        );
        return new GeoShapeQueryBuilder("test", geometry);
    }

    @Override
    protected void doAssertLuceneQuery(GeoShapeQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertThat(true, equalTo(query instanceof ConstantScoreQuery));
        Query geoShapeQuery = ((ConstantScoreQuery) query).getQuery();
        MappedFieldType fieldType = context.getFieldType("test");
        boolean IndexOrDocValuesQuery = fieldType.hasDocValues();
        assertThat(IndexOrDocValuesQuery, equalTo(geoShapeQuery instanceof IndexOrDocValuesQuery));
    }

    @Override
    protected Map<String, String> getObjectsHoldingArbitraryContent() {
        // shape field can accept any element but expects a type
        return Collections.singletonMap("shape", "Required [type]");
    }
}
