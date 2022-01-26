/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.CoreMatchers;

import java.util.Collection;
import java.util.Collections;

public class RankFeatureMetaFieldMapperTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new MapperExtrasPlugin());
    }

    public void testBasics() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("field")
                .field("type", "rank_feature")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        Mapping parsedMapping = createMapperService("type", mapping).parseMapping("type", new CompressedXContent(mapping), false);
        assertEquals(mapping, parsedMapping.toCompressedXContent().toString());
        assertNotNull(parsedMapping.getMetadataMapperByClass(RankFeatureMetaFieldMapper.class));
    }

    /**
     * Check that meta-fields are picked from plugins (in this case MapperExtrasPlugin),
     * and parsing of a document fails if the document contains these meta-fields.
     */
    public void testDocumentParsingFailsOnMetaField() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc").endObject().endObject());
        DocumentMapper mapper = createMapperService("_doc", mapping).merge(
            "_doc",
            new CompressedXContent(mapping),
            MapperService.MergeReason.MAPPING_UPDATE
        );
        String rfMetaField = RankFeatureMetaFieldMapper.CONTENT_TYPE;
        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(rfMetaField, 0).endObject());
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(new SourceToParse("test", "_doc", "1", bytes, XContentType.JSON))
        );
        assertThat(
            e.getCause().getMessage(),
            CoreMatchers.containsString("Field [" + rfMetaField + "] is a metadata field and cannot be added inside a document.")
        );
    }
}
