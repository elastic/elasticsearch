/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadataTests;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MappingLookupInferenceFieldMapperTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new TestInferenceFieldMapperPlugin());
    }

    public void testInferenceFieldMapper() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("non_inference_field").field("type", "text").endObject();
            b.startObject("another_non_inference_field").field("type", "text").endObject();
            b.startObject("inference_field").field("type", TestInferenceFieldMapper.CONTENT_TYPE).endObject();
            b.startObject("another_inference_field").field("type", TestInferenceFieldMapper.CONTENT_TYPE).endObject();
        }));

        Map<String, InferenceFieldMetadata> inferenceFieldMetadataMap = mapperService.mappingLookup().inferenceFields();
        assertThat(inferenceFieldMetadataMap.keySet(), hasSize(2));

        InferenceFieldMetadata inferenceFieldMetadata = inferenceFieldMetadataMap.get("inference_field");
        assertThat(inferenceFieldMetadata.getInferenceId(), equalTo(TestInferenceFieldMapper.INFERENCE_ID));
        assertThat(inferenceFieldMetadata.getSourceFields(), arrayContaining("inference_field"));

        inferenceFieldMetadata = inferenceFieldMetadataMap.get("another_inference_field");
        assertThat(inferenceFieldMetadata.getInferenceId(), equalTo(TestInferenceFieldMapper.INFERENCE_ID));
        assertThat(inferenceFieldMetadata.getSourceFields(), arrayContaining("another_inference_field"));
    }

    public void testInferenceFieldMapperWithCopyTo() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("non_inference_field");
            {
                b.field("type", "text");
                b.array("copy_to", "inference_field");
            }
            b.endObject();
            b.startObject("another_non_inference_field");
            {
                b.field("type", "text");
                b.array("copy_to", "inference_field");
            }
            b.endObject();
            b.startObject("inference_field").field("type", TestInferenceFieldMapper.CONTENT_TYPE).endObject();
            b.startObject("independent_field").field("type", "text").endObject();
        }));

        Map<String, InferenceFieldMetadata> inferenceFieldMetadataMap = mapperService.mappingLookup().inferenceFields();
        assertThat(inferenceFieldMetadataMap.keySet(), hasSize(1));

        InferenceFieldMetadata inferenceFieldMetadata = inferenceFieldMetadataMap.get("inference_field");
        assertThat(inferenceFieldMetadata.getInferenceId(), equalTo(TestInferenceFieldMapper.INFERENCE_ID));
        assertThat(
            inferenceFieldMetadata.getSourceFields(),
            arrayContainingInAnyOrder("another_non_inference_field", "inference_field", "non_inference_field")
        );
    }

    private static class TestInferenceFieldMapperPlugin extends Plugin implements MapperPlugin {

        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Map.of(TestInferenceFieldMapper.CONTENT_TYPE, TestInferenceFieldMapper.PARSER);
        }
    }

    private static class TestInferenceFieldMapper extends FieldMapper implements InferenceFieldMapper {

        public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));
        public static final String INFERENCE_ID = "test_inference_id";
        public static final String SEARCH_INFERENCE_ID = "test_search_inference_id";
        public static final String CONTENT_TYPE = "test_inference_field";

        TestInferenceFieldMapper(String simpleName) {
            super(simpleName, new TestInferenceFieldMapperFieldType(simpleName), BuilderParams.empty());
        }

        @Override
        public InferenceFieldMetadata getMetadata(Set<String> sourcePaths) {
            return new InferenceFieldMetadata(
                fullPath(),
                INFERENCE_ID,
                SEARCH_INFERENCE_ID,
                sourcePaths.toArray(new String[0]),
                InferenceFieldMetadataTests.generateRandomChunkingSettings()
            );
        }

        @Override
        protected void parseCreateField(DocumentParserContext context) {}

        @Override
        public Builder getMergeBuilder() {
            return new Builder(leafName());
        }

        @Override
        protected String contentType() {
            return CONTENT_TYPE;
        }

        public static class Builder extends FieldMapper.Builder {

            @Override
            protected Parameter<?>[] getParameters() {
                return new Parameter<?>[0];
            }

            Builder(String name) {
                super(name);
            }

            @Override
            public FieldMapper build(MapperBuilderContext context) {
                return new TestInferenceFieldMapper(leafName());
            }
        }

        private static class TestInferenceFieldMapperFieldType extends MappedFieldType {

            TestInferenceFieldMapperFieldType(String name) {
                super(name, false, false, false, TextSearchInfo.NONE, Map.of());
            }

            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                return null;
            }

            @Override
            public String typeName() {
                return CONTENT_TYPE;
            }

            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                return null;
            }
        }
    }

}
