/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.index.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;

public class SingletonKeywordFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(XPackPlugin.class);
        return plugins;
    }

    public void testDefaults() throws Exception {
        IndexService indexService = createIndex("test");
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "singleton_keyword")
                .field("value", "foo").endObject().endObject().endObject().endObject());
        DocumentMapper mapper = indexService.mapperService().merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));
        assertNull(doc.rootDoc().getField("field"));

        source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "foo").endObject());
        doc = mapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));
        assertNull(doc.rootDoc().getField("field"));

        BytesReference illegalSource = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("field", "bar").endObject());
        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", illegalSource, XContentType.JSON)));
        assertEquals("[singleton_keyword] field [field] only accepts values that are equal to the wrapped value [foo], but got [bar]",
                e.getCause().getMessage());
    }

}
