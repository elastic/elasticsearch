/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DynamicFieldType;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.indices.DateFieldRangeInfo;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.index.query.CoordinatorRewriteContext.TIER_FIELD_TYPE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class QueryRewriteContextTests extends ESTestCase {

    public void testGetTierPreference() {
        {
            // cold->hot tier preference
            IndexMetadata metadata = newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(DataTier.TIER_PREFERENCE, "data_cold,data_warm,data_hot")
                    .build()
            );
            QueryRewriteContext context = new QueryRewriteContext(
                parserConfig(),
                null,
                System::currentTimeMillis,
                null,
                MappingLookup.EMPTY,
                Collections.emptyMap(),
                new IndexSettings(metadata, Settings.EMPTY),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                false
            );

            assertThat(context.getTierPreference(), is("data_cold"));
        }

        {
            // missing tier preference
            IndexMetadata metadata = newIndexMeta(
                "index",
                Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build()
            );
            QueryRewriteContext context = new QueryRewriteContext(
                parserConfig(),
                null,
                System::currentTimeMillis,
                null,
                MappingLookup.EMPTY,
                Collections.emptyMap(),
                new IndexSettings(metadata, Settings.EMPTY),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                false
            );

            assertThat(context.getTierPreference(), is(nullValue()));
        }

        {
            // coordinator rewrite context
            CoordinatorRewriteContext coordinatorRewriteContext = new CoordinatorRewriteContext(
                parserConfig(),
                null,
                System::currentTimeMillis,
                new DateFieldRangeInfo(null, null, new DateFieldMapper.DateFieldType(IndexMetadata.EVENT_INGESTED_FIELD_NAME), null),
                "data_frozen"
            );

            assertThat(coordinatorRewriteContext.getTierPreference(), is("data_frozen"));
        }

        {
            // coordinator rewrite context empty tier
            CoordinatorRewriteContext coordinatorRewriteContext = new CoordinatorRewriteContext(
                parserConfig(),
                null,
                System::currentTimeMillis,
                new DateFieldRangeInfo(null, null, new DateFieldMapper.DateFieldType(IndexMetadata.EVENT_INGESTED_FIELD_NAME), null),
                ""
            );

            assertThat(coordinatorRewriteContext.getTierPreference(), is(nullValue()));
        }

        {
            // null date field range info
            CoordinatorRewriteContext coordinatorRewriteContext = new CoordinatorRewriteContext(
                parserConfig(),
                null,
                System::currentTimeMillis,
                null,
                "data_frozen"
            );
            assertThat(coordinatorRewriteContext.getFieldRange(IndexMetadata.EVENT_INGESTED_FIELD_NAME), is(nullValue()));
            assertThat(coordinatorRewriteContext.getFieldRange(IndexMetadata.EVENT_INGESTED_FIELD_NAME), is(nullValue()));
            // tier field doesn't have a range
            assertThat(coordinatorRewriteContext.getFieldRange(CoordinatorRewriteContext.TIER_FIELD_NAME), is(nullValue()));
            assertThat(coordinatorRewriteContext.getFieldType(IndexMetadata.EVENT_INGESTED_FIELD_NAME), is(nullValue()));
            assertThat(coordinatorRewriteContext.getFieldType(IndexMetadata.EVENT_INGESTED_FIELD_NAME), is(nullValue()));
            // _tier field type should still work even without the data field info
            assertThat(coordinatorRewriteContext.getFieldType(CoordinatorRewriteContext.TIER_FIELD_NAME), is(TIER_FIELD_TYPE));
        }
    }

    /**
     * Tests that {@link QueryRewriteContext#isMappedField} recognises sub-fields of
     * {@link MetadataFieldMapper} instances that implement {@link DynamicFieldType}.
     * This mirrors serverless metadata fields such as {@code _project._alias} whose
     * root mapper ({@code _project}) is a {@link MetadataFieldMapper} providing
     * dynamic child types at query time.
     */
    public void testIsMappedFieldMetadataDynamicSubfield() {
        DynamicMetadataFieldMapper metaMapper = new DynamicMetadataFieldMapper();
        RootObjectMapper root = new RootObjectMapper.Builder("_doc").build(MapperBuilderContext.root(false, false));
        Mapping mapping = new Mapping(root, new MetadataFieldMapper[] { metaMapper }, Map.of());
        MappingLookup lookup = MappingLookup.fromMapping(mapping, IndexMode.STANDARD);

        IndexMetadata indexMeta = newIndexMeta("test-idx", Settings.builder().build());
        QueryRewriteContext ctx = new QueryRewriteContext(
            parserConfig(),
            null,
            System::currentTimeMillis,
            null,
            lookup,
            Collections.emptyMap(),
            new IndexSettings(indexMeta, Settings.EMPTY),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            false
        );

        // The root metadata field is in fullNameToFieldType, so it maps directly.
        assertTrue(ctx.isMappedField(DynamicMetadataFieldMapper.NAME));
        // Sub-field whose key exists in the DynamicFieldType.
        assertTrue(ctx.isMappedField(DynamicMetadataFieldMapper.NAME + "." + DynamicMetadataFieldMapper.PRESENT_KEY));
        // Sub-field whose key the DynamicFieldType does not know about.
        assertFalse(ctx.isMappedField(DynamicMetadataFieldMapper.NAME + ".absent_key"));
        // A completely unmapped field.
        assertFalse(ctx.isMappedField("unmapped_field"));
        // Sub-field of a completely unmapped parent.
        assertFalse(ctx.isMappedField("unmapped_parent.sub_key"));
    }

    /**
     * A minimal {@link MetadataFieldMapper} whose field type implements {@link DynamicFieldType},
     * used to verify that {@link QueryRewriteContext#isMappedField} correctly recognises
     * sub-fields provided by metadata mappers.
     */
    private static class DynamicMetadataFieldMapper extends MetadataFieldMapper {
        static final String NAME = "_test_meta";
        static final String PRESENT_KEY = "present_key";

        DynamicMetadataFieldMapper() {
            super(new DynamicMetaFieldType());
        }

        @Override
        protected String contentType() {
            return NAME;
        }

        @Override
        protected void parseCreateField(DocumentParserContext context) throws IOException {}

        private static class DynamicMetaFieldType extends MappedFieldType implements DynamicFieldType {
            DynamicMetaFieldType() {
                super(NAME, IndexType.NONE, false, Map.of());
            }

            @Override
            public MappedFieldType getChildFieldType(String path) {
                if (PRESENT_KEY.equals(path)) {
                    return new MockFieldMapper.FakeFieldType(NAME + "." + path);
                }
                return null;
            }

            @Override
            public String typeName() {
                return NAME;
            }

            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                throw new UnsupportedOperationException();
            }
        }
    }

    public static IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        return IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 1).put(indexSettings)).build();
    }

}
