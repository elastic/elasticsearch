/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.runtimefields.RuntimeFields;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class RuntimeFieldIndexShadowTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singleton(new RuntimeFields());
    }

    public void testRuntimeFieldShadowsObject() throws IOException {

        // A runtime field defined in the mappings should automatically
        // stop an identically named field in a document from being indexed

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "true");
            b.startObject("runtime");
            {
                b.startObject("location").field("type", "geo_point").endObject();
                b.startObject("country").field("type", "keyword").endObject();
            }
            b.endObject();
            b.startObject("properties");
            {
                b.startObject("timestamp").field("type", "date").endObject();
                b.startObject("concrete").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("timestamp", "1998-04-30T14:30:17-05:00");
            b.startObject("location");
            {
                b.field("lat", 13.5);
                b.field("lon", 34.89);
            }
            b.endObject();
            b.field("country", "de");
            b.field("concrete", "foo");
        }));

        assertNotNull(doc.rootDoc().getField("timestamp"));
        assertNotNull(doc.rootDoc().getField("_source"));
        assertNull(doc.rootDoc().getField("location.lat"));
        assertNotNull(doc.rootDoc().getField("concrete"));
        assertNull(doc.rootDoc().getField("country"));

    }

}
