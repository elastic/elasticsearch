/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.CoreMatchers;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

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

        Mapping parsedMapping = createMapperService(mapping).parseMapping("type", new CompressedXContent(mapping));
        assertEquals(mapping, parsedMapping.toCompressedXContent().toString());
        assertNotNull(parsedMapping.getMetadataMapperByClass(RankFeatureMetaFieldMapper.class));
    }

    /**
     * Check that meta-fields are picked from plugins (in this case MapperExtrasPlugin),
     * and parsing of a document fails if the document contains these meta-fields.
     */
    public void testDocumentParsingFailsOnMetaField() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc").endObject().endObject());
        DocumentMapper mapper = createMapperService(mapping).merge(
            "_doc",
            new CompressedXContent(mapping),
            MapperService.MergeReason.MAPPING_UPDATE
        );
        String rfMetaField = RankFeatureMetaFieldMapper.CONTENT_TYPE;
        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(rfMetaField, 0).endObject());
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(new SourceToParse("1", bytes, XContentType.JSON))
        );
        assertThat(
            e.getCause().getMessage(),
            CoreMatchers.containsString("Field [" + rfMetaField + "] is a metadata field and cannot be added inside a document.")
        );
    }

    public void testFieldHasValueIf_featureIsPresentInFieldInfos() {
        MappedFieldType fieldType = new RankFeatureMetaFieldMapper.RankFeatureMetaFieldType();
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { getFieldInfoWithName("_feature") });
        assertTrue(fieldType.fieldHasValue(fieldInfos));
    }

    public void testFieldEmptyIfNameIsPresentInFieldInfos() {
        MappedFieldType fieldType = new RankFeatureMetaFieldMapper.RankFeatureMetaFieldType();
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { getFieldInfoWithName("field") });
        assertFalse(fieldType.fieldHasValue(fieldInfos));
    }

    public void testFieldEmptyIfEmptyFieldInfos() {
        MappedFieldType fieldType = new RankFeatureMetaFieldMapper.RankFeatureMetaFieldType();
        FieldInfos fieldInfos = FieldInfos.EMPTY;
        assertFalse(fieldType.fieldHasValue(fieldInfos));
    }

    private FieldInfo getFieldInfoWithName(String name) {
        return new FieldInfo(
            name,
            1,
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IndexOptions.NONE,
            DocValuesType.NONE,
            -1,
            new HashMap<>(),
            1,
            1,
            1,
            1,
            VectorEncoding.BYTE,
            VectorSimilarityFunction.COSINE,
            randomBoolean()
        );
    }
}
