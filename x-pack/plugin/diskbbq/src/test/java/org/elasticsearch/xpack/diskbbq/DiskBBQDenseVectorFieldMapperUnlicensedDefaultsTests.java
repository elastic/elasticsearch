/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class DiskBBQDenseVectorFieldMapperUnlicensedDefaultsTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new BasicLicenseStateDiskBBQPlugin(Settings.EMPTY));
    }

    public void testDoesNotDefaultToBBQDiskWhenUnlicensed() throws IOException {
        MapperService mapperService = createMapperService(getVersion(), Settings.EMPTY, () -> true, fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", 384);
            b.field("index", true);
            b.field("similarity", "dot_product");
        }));

        DenseVectorFieldMapper mapper = (DenseVectorFieldMapper) mapperService.mappingLookup().getMapper("field");
        assertNotNull(mapper);
        assertThat(mapper.fieldType().getIndexOptions(), instanceOf(DenseVectorFieldMapper.BBQHnswIndexOptions.class));
        assertEquals(DenseVectorFieldMapper.VectorIndexType.BBQ_HNSW, mapper.fieldType().getIndexOptions().getType());
    }
}
