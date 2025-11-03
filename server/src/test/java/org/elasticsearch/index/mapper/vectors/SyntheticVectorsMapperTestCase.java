/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public abstract class SyntheticVectorsMapperTestCase extends MapperTestCase {

    protected abstract Object getSampleValueForDocument(boolean binaryFormat);

    public void testSyntheticVectorsMinimalValidDocument() throws IOException {
        for (XContentType type : XContentType.values()) {
            BytesReference source = generateRandomDoc(type, true, true, false, false, false);
            assertSyntheticVectors(buildVectorMapping(), source, type);
        }
    }

    public void testSyntheticVectorsFullDocument() throws IOException {
        for (XContentType type : XContentType.values()) {
            BytesReference source = generateRandomDoc(type, true, true, true, true, false);
            assertSyntheticVectors(buildVectorMapping(), source, type);
        }
    }

    public void testSyntheticVectorsWithUnmappedFields() throws IOException {
        for (XContentType type : XContentType.values()) {
            BytesReference source = generateRandomDoc(type, true, true, true, true, true);
            assertSyntheticVectors(buildVectorMapping(), source, type);
        }
    }

    public void testSyntheticVectorsMissingRootFields() throws IOException {
        for (XContentType type : XContentType.values()) {
            BytesReference source = generateRandomDoc(type, false, false, false, false, false);
            assertSyntheticVectors(buildVectorMapping(), source, type);
        }
    }

    public void testSyntheticVectorsPartialNestedContent() throws IOException {
        for (XContentType type : XContentType.values()) {
            BytesReference source = generateRandomDoc(type, true, true, true, false, false);
            assertSyntheticVectors(buildVectorMapping(), source, type);
        }
    }

    public void testFlatPathDocument() throws IOException {
        for (XContentType type : XContentType.values()) {
            BytesReference source = generateRandomDocWithFlatPath(type);
            assertSyntheticVectors(buildVectorMapping(), source, type);
        }
    }

    private String buildVectorMapping() throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject(); // root
            builder.startObject("_doc");
            builder.field("dynamic", "false");

            builder.startObject("properties");

            // field
            builder.startObject("field");
            builder.field("type", "keyword");
            builder.endObject();

            // emb
            builder.startObject("emb");
            minimalMapping(builder);
            builder.endObject();

            // another_field
            builder.startObject("another_field");
            builder.field("type", "keyword");
            builder.endObject();

            // obj
            builder.startObject("obj");
            builder.startObject("properties");

            // nested
            builder.startObject("nested");
            builder.field("type", "nested");
            builder.startObject("properties");

            // nested.field
            builder.startObject("field");
            builder.field("type", "keyword");
            builder.endObject();

            // nested.emb
            builder.startObject("emb");
            minimalMapping(builder);
            builder.endObject();

            // double_nested
            builder.startObject("double_nested");
            builder.field("type", "nested");
            builder.startObject("properties");

            // double_nested.field
            builder.startObject("field");
            builder.field("type", "keyword");
            builder.endObject();

            // double_nested.emb
            builder.startObject("emb");
            minimalMapping(builder);
            builder.endObject();

            builder.endObject(); // double_nested.properties
            builder.endObject(); // double_nested

            builder.endObject(); // nested.properties
            builder.endObject(); // nested

            builder.endObject(); // obj.properties
            builder.endObject(); // obj

            builder.endObject(); // properties
            builder.endObject(); // _doc
            builder.endObject(); // root

            return Strings.toString(builder);
        }
    }

    private BytesReference generateRandomDoc(
        XContentType xContentType,
        boolean includeRootField,
        boolean includeVector,
        boolean includeNested,
        boolean includeDoubleNested,
        boolean includeUnmapped
    ) throws IOException {
        try (var builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();

            if (includeRootField) {
                builder.field("field", randomAlphaOfLengthBetween(1, 2));
            }

            if (includeVector) {
                builder.field("emb", getSampleValueForDocument(false));
                // builder.array("emb", new float[] { 1, 2, 3 });
            }

            if (includeUnmapped) {
                builder.field("unmapped_field", "extra");
            }

            builder.startObject("obj");
            if (includeNested) {
                builder.startArray("nested");

                // Entry with just a field
                builder.startObject();
                builder.field("field", randomAlphaOfLengthBetween(3, 6));
                builder.endObject();

                // Empty object
                builder.startObject();
                builder.endObject();

                // Entry with emb and double_nested
                if (includeDoubleNested) {
                    builder.startObject();
                    // builder.array("emb", new float[] { 1, 2, 3 });
                    builder.field("emb", getSampleValueForDocument(false));
                    builder.field("field", "nested_val");
                    builder.startArray("double_nested");
                    for (int i = 0; i < 2; i++) {
                        builder.startObject();
                        // builder.array("emb", new float[] { 1, 2, 3 });
                        builder.field("emb", getSampleValueForDocument(false));
                        builder.field("field", "dn_field");
                        builder.endObject();
                    }
                    builder.endArray();
                    builder.endObject();
                }

                builder.endArray();
            }
            builder.endObject();

            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    private BytesReference generateRandomDocWithFlatPath(XContentType xContentType) throws IOException {
        try (var builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();

            // Root-level fields
            builder.field("field", randomAlphaOfLengthBetween(1, 2));
            builder.field("emb", getSampleValueForDocument(false));
            builder.field("another_field", randomAlphaOfLengthBetween(3, 5));

            // Simulated flattened "obj.nested"
            builder.startObject("obj.nested");

            builder.field("field", randomAlphaOfLengthBetween(4, 8));
            builder.field("emb", getSampleValueForDocument(false));

            builder.startArray("double_nested");
            for (int i = 0; i < randomIntBetween(1, 2); i++) {
                builder.startObject();
                builder.field("field", randomAlphaOfLengthBetween(4, 8));
                builder.field("emb", getSampleValueForDocument(false));
                builder.endObject();
            }
            builder.endArray();

            builder.endObject(); // end obj.nested

            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    private void assertSyntheticVectors(String mapping, BytesReference source, XContentType xContentType) throws IOException {
        var settings = Settings.builder().put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), true).build();
        MapperService mapperService = createMapperService(settings, mapping);
        var parsedDoc = mapperService.documentMapper().parse(new SourceToParse("0", source, xContentType));
        try (var directory = newDirectory()) {
            IndexWriterConfig config = newIndexWriterConfig(random(), new StandardAnalyzer());
            try (var iw = new RandomIndexWriter(random(), directory, config)) {
                parsedDoc.updateSeqID(0, 1);
                parsedDoc.version().setLongValue(0);
                iw.addDocuments(parsedDoc.docs());
            }
            try (var indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                var provider = SourceProvider.fromLookup(
                    mapperService.mappingLookup(),
                    null,
                    mapperService.getMapperMetrics().sourceFieldMetrics()
                );
                var searchSource = provider.getSource(indexReader.leaves().get(0), parsedDoc.docs().size() - 1);
                assertToXContentEquivalent(source, searchSource.internalSourceRef(), xContentType);
            }
        }
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of();
    }
}
