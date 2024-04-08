/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.search.suggest.completion.context.ContextBuilder;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.GeoContextMapping;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.geometry.utils.Geohash.addNeighborsAtLevel;
import static org.elasticsearch.search.suggest.completion.CategoryContextMappingTests.assertContextSuggestFields;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

public class GeoContextMappingTests extends MapperServiceTestCase {

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            Map.of(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()),
                "simple",
                new NamedAnalyzer("simple", AnalyzerScope.INDEX, new SimpleAnalyzer())
            )
        );
    }

    public void testIndexingWithNoContexts() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("completion")
            .field("type", "completion")
            .startArray("contexts")
            .startObject()
            .field("name", "ctx")
            .field("type", "geo")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperService mapperService = createMapperService(mapping);
        MappedFieldType completionFieldType = mapperService.fieldType("completion");
        ParsedDocument parsedDocument = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(
                        jsonBuilder().startObject()
                            .startArray("completion")
                            .startObject()
                            .array("input", "suggestion1", "suggestion2")
                            .field("weight", 3)
                            .endObject()
                            .startObject()
                            .array("input", "suggestion3", "suggestion4")
                            .field("weight", 4)
                            .endObject()
                            .startObject()
                            .array("input", "suggestion5", "suggestion6", "suggestion7")
                            .field("weight", 5)
                            .endObject()
                            .endArray()
                            .endObject()
                    ),
                    XContentType.JSON
                )
            );
        List<IndexableField> fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 7);
    }

    public void testIndexingWithSimpleContexts() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("completion")
            .field("type", "completion")
            .startArray("contexts")
            .startObject()
            .field("name", "ctx")
            .field("type", "geo")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperService mapperService = createMapperService(mapping);
        MappedFieldType completionFieldType = mapperService.fieldType("completion");
        ParsedDocument parsedDocument = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(
                        jsonBuilder().startObject()
                            .startArray("completion")
                            .startObject()
                            .array("input", "suggestion5", "suggestion6", "suggestion7")
                            .startObject("contexts")
                            .startObject("ctx")
                            .field("lat", 43.6624803)
                            .field("lon", -79.3863353)
                            .endObject()
                            .endObject()
                            .field("weight", 5)
                            .endObject()
                            .endArray()
                            .endObject()
                    ),
                    XContentType.JSON
                )
            );
        List<IndexableField> fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithContextList() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("completion")
            .field("type", "completion")
            .startArray("contexts")
            .startObject()
            .field("name", "ctx")
            .field("type", "geo")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperService mapperService = createMapperService(mapping);
        MappedFieldType completionFieldType = mapperService.fieldType("completion");
        ParsedDocument parsedDocument = mapperService.documentMapper()
            .parse(
                new SourceToParse(
                    "1",
                    BytesReference.bytes(
                        jsonBuilder().startObject()
                            .startObject("completion")
                            .array("input", "suggestion5", "suggestion6", "suggestion7")
                            .startObject("contexts")
                            .startArray("ctx")
                            .startObject()
                            .field("lat", 43.6624803)
                            .field("lon", -79.3863353)
                            .endObject()
                            .startObject()
                            .field("lat", 43.6624718)
                            .field("lon", -79.3873227)
                            .endObject()
                            .endArray()
                            .endObject()
                            .field("weight", 5)
                            .endObject()
                            .endObject()
                    ),
                    XContentType.JSON
                )
            );
        List<IndexableField> fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithMultipleContexts() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("completion")
            .field("type", "completion")
            .startArray("contexts")
            .startObject()
            .field("name", "loc1")
            .field("type", "geo")
            .endObject()
            .startObject()
            .field("name", "loc2")
            .field("type", "geo")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        MapperService mapperService = createMapperService(mapping);
        MappedFieldType completionFieldType = mapperService.fieldType("completion");
        XContentBuilder builder = jsonBuilder().startObject()
            .startArray("completion")
            .startObject()
            .array("input", "suggestion5", "suggestion6", "suggestion7")
            .field("weight", 5)
            .startObject("contexts")
            .array("loc1", "ezs42e44yx96")
            .array("loc2", "wh0n9447fwrc")
            .endObject()
            .endObject()
            .endArray()
            .endObject();
        ParsedDocument parsedDocument = mapperService.documentMapper()
            .parse(new SourceToParse("1", BytesReference.bytes(builder), XContentType.JSON));
        List<IndexableField> fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testMalformedGeoField() throws Exception {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject();
        mapping.startObject("_doc");
        mapping.startObject("properties");
        mapping.startObject("pin");
        String type = randomFrom("text", "keyword", "long");
        mapping.field("type", type);
        mapping.endObject();
        mapping.startObject("suggestion");
        mapping.field("type", "completion");
        mapping.field("analyzer", "simple");

        mapping.startArray("contexts");
        mapping.startObject();
        mapping.field("name", "st");
        mapping.field("type", "geo");
        mapping.field("path", "pin");
        mapping.field("precision", 5);
        mapping.endObject();
        mapping.endArray();

        mapping.endObject();

        mapping.endObject();
        mapping.endObject();
        mapping.endObject();

        ElasticsearchParseException ex = expectThrows(ElasticsearchParseException.class, () -> createMapperService(mapping));

        assertThat(ex.getMessage(), equalTo("field [pin] referenced in context [st] must be mapped to geo_point, found [" + type + "]"));
    }

    public void testMissingGeoField() throws Exception {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject();
        mapping.startObject("_doc");
        mapping.startObject("properties");
        mapping.startObject("suggestion");
        mapping.field("type", "completion");
        mapping.field("analyzer", "simple");

        mapping.startArray("contexts");
        mapping.startObject();
        mapping.field("name", "st");
        mapping.field("type", "geo");
        mapping.field("path", "pin");
        mapping.field("precision", 5);
        mapping.endObject();
        mapping.endArray();

        mapping.endObject();

        mapping.endObject();
        mapping.endObject();
        mapping.endObject();

        ElasticsearchParseException ex = expectThrows(ElasticsearchParseException.class, () -> createMapperService(mapping));

        assertThat(ex.getMessage(), equalTo("field [pin] referenced in context [st] is not defined in the mapping"));
    }

    public void testParsingQueryContextBasic() throws Exception {
        XContentBuilder builder = jsonBuilder().value("ezs42e44yx96");
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
        assertThat(internalQueryContexts.size(), equalTo(1 + 8));
        Collection<String> locations = new ArrayList<>();
        locations.add("ezs42e");
        addNeighborsAtLevel("ezs42e", GeoContextMapping.DEFAULT_PRECISION, locations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            assertThat(internalQueryContext.context(), is(in(locations)));
            assertThat(internalQueryContext.boost(), equalTo(1));
            assertThat(internalQueryContext.isPrefix(), equalTo(false));
        }
    }

    public void testParsingQueryContextGeoPoint() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("lat", 23.654242).field("lon", 90.047153).endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
        assertThat(internalQueryContexts.size(), equalTo(1 + 8));
        Collection<String> locations = new ArrayList<>();
        locations.add("wh0n94");
        addNeighborsAtLevel("wh0n94", GeoContextMapping.DEFAULT_PRECISION, locations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            assertThat(internalQueryContext.context(), is(in(locations)));
            assertThat(internalQueryContext.boost(), equalTo(1));
            assertThat(internalQueryContext.isPrefix(), equalTo(false));
        }
    }

    public void testParsingQueryContextObject() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
            .startObject("context")
            .field("lat", 23.654242)
            .field("lon", 90.047153)
            .endObject()
            .field("boost", 10)
            .array("neighbours", 1, 2, 3)
            .endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
        assertThat(internalQueryContexts.size(), equalTo(1 + 1 + 8 + 1 + 8 + 1 + 8));
        Collection<String> locations = new ArrayList<>();
        locations.add("wh0n94");
        locations.add("w");
        addNeighborsAtLevel("w", 1, locations);
        locations.add("wh");
        addNeighborsAtLevel("wh", 2, locations);
        locations.add("wh0");
        addNeighborsAtLevel("wh0", 3, locations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            assertThat(internalQueryContext.context(), is(in(locations)));
            assertThat(internalQueryContext.boost(), equalTo(10));
            assertThat(
                internalQueryContext.isPrefix(),
                equalTo(internalQueryContext.context().length() < GeoContextMapping.DEFAULT_PRECISION)
            );
        }
    }

    public void testParsingQueryContextObjectArray() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray()
            .startObject()
            .startObject("context")
            .field("lat", 23.654242)
            .field("lon", 90.047153)
            .endObject()
            .field("boost", 10)
            .array("neighbours", 1, 2, 3)
            .endObject()
            .startObject()
            .startObject("context")
            .field("lat", 22.337374)
            .field("lon", 92.112583)
            .endObject()
            .field("boost", 2)
            .array("neighbours", 5)
            .endObject()
            .endArray();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
        assertThat(internalQueryContexts.size(), equalTo(1 + 1 + 8 + 1 + 8 + 1 + 8 + 1 + 1 + 8));
        Collection<String> firstLocations = new ArrayList<>();
        firstLocations.add("wh0n94");
        firstLocations.add("w");
        addNeighborsAtLevel("w", 1, firstLocations);
        firstLocations.add("wh");
        addNeighborsAtLevel("wh", 2, firstLocations);
        firstLocations.add("wh0");
        addNeighborsAtLevel("wh0", 3, firstLocations);
        Collection<String> secondLocations = new ArrayList<>();
        secondLocations.add("w5cx04");
        secondLocations.add("w5cx0");
        addNeighborsAtLevel("w5cx0", 5, secondLocations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            if (firstLocations.contains(internalQueryContext.context())) {
                assertThat(internalQueryContext.boost(), equalTo(10));
            } else if (secondLocations.contains(internalQueryContext.context())) {
                assertThat(internalQueryContext.boost(), equalTo(2));
            } else {
                fail(internalQueryContext.context() + " was not expected");
            }
            assertThat(
                internalQueryContext.isPrefix(),
                equalTo(internalQueryContext.context().length() < GeoContextMapping.DEFAULT_PRECISION)
            );
        }
    }

    public void testParsingQueryContextMixed() throws Exception {
        XContentBuilder builder = jsonBuilder().startArray()
            .startObject()
            .startObject("context")
            .field("lat", 23.654242)
            .field("lon", 90.047153)
            .endObject()
            .field("boost", 10)
            .array("neighbours", 1, 2)
            .endObject()
            .startObject()
            .field("lat", 22.337374)
            .field("lon", 92.112583)
            .endObject()
            .endArray();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(parser);
        assertThat(internalQueryContexts.size(), equalTo(1 + 1 + 8 + 1 + 8 + 1 + 8));
        Collection<String> firstLocations = new ArrayList<>();
        firstLocations.add("wh0n94");
        firstLocations.add("w");
        addNeighborsAtLevel("w", 1, firstLocations);
        firstLocations.add("wh");
        addNeighborsAtLevel("wh", 2, firstLocations);
        Collection<String> secondLocations = new ArrayList<>();
        secondLocations.add("w5cx04");
        addNeighborsAtLevel("w5cx04", 6, secondLocations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            if (firstLocations.contains(internalQueryContext.context())) {
                assertThat(internalQueryContext.boost(), equalTo(10));
            } else if (secondLocations.contains(internalQueryContext.context())) {
                assertThat(internalQueryContext.boost(), equalTo(1));
            } else {
                fail(internalQueryContext.context() + " was not expected");
            }
            assertThat(
                internalQueryContext.isPrefix(),
                equalTo(internalQueryContext.context().length() < GeoContextMapping.DEFAULT_PRECISION)
            );
        }
    }
}
