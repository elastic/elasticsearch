/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.instanceOf;

public class GeoShapeFieldProjectionTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, SpatialPlugin.class, XPackPlugin.class);
    }

    public void testCRSParsing() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .startObject("crs")
            .field("type", "name")
            .startObject("properties")
            .field("name", "EPSG:32614")
            .endObject()
            .endObject()
            .endObject().endObject()
            .endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type1", new CompressedXContent(mapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper("location");
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));
    }
}
