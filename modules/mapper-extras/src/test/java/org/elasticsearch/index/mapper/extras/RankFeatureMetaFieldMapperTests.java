/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldAliasMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.CoreMatchers;

import java.util.Collection;
import java.util.Collections;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(new SourceToParse("1", bytes, XContentType.JSON))
        );
        assertThat(
            e.getCause().getMessage(),
            CoreMatchers.containsString("Field [" + rfMetaField + "] is a metadata field and cannot be added inside a document.")
        );
    }

    public void testAliasInternalField() {
        MappingParserContext context = mock(MappingParserContext.class);
        when(context.indexVersionCreated()).thenReturn(Version.CURRENT);
        FieldMapper mapper = RankFeatureMetaFieldMapper.PARSER.getDefault(context);
        FieldAliasMapper invalidAlias = new FieldAliasMapper("invalid-alias", "invalid-alias", mapper.name());

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            RootObjectMapper.Builder builder = new RootObjectMapper.Builder("_doc");
            Mapping mapping = new Mapping(builder.build(MapperBuilderContext.ROOT), new MetadataFieldMapper[0], Collections.emptyMap());
            MappingLookup mappers = MappingLookup.fromMappers(mapping, singletonList(mapper), emptyList(), singletonList(invalidAlias));
            invalidAlias.validate(mappers);
        });

        assertEquals(
            "Invalid [path] value [_feature] for field alias [invalid-alias]: an alias" + " cannot refer to an internal field.",
            e.getMessage()
        );
    }
}
