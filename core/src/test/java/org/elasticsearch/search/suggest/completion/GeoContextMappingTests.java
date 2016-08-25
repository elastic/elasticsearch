/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.suggest.completion.context.ContextBuilder;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.GeoContextMapping;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.geo.GeoHashUtils.addNeighbors;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.suggest.completion.CategoryContextMappingTests.assertContextSuggestFields;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isIn;

public class GeoContextMappingTests extends ESSingleNodeTestCase {

    public void testIndexingWithNoContexts() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "geo")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1", jsonBuilder()
                .startObject()
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
                .field("input", "suggestion5", "suggestion6", "suggestion7")
                .field("weight", 5)
                .endObject()
                .endArray()
                .endObject()
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 7);
    }

    public void testIndexingWithSimpleContexts() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "geo")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1", jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .field("input", "suggestion5", "suggestion6", "suggestion7")
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
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithContextList() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
                .field("type", "completion")
                .startArray("contexts")
                .startObject()
                .field("name", "ctx")
                .field("type", "geo")
                .endObject()
                .endArray()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1", jsonBuilder()
                .startObject()
                    .startObject("completion")
                        .field("input", "suggestion5", "suggestion6", "suggestion7")
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
                .bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testIndexingWithMultipleContexts() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("completion")
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
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));
        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        MappedFieldType completionFieldType = fieldMapper.fieldType();
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startArray("completion")
                .startObject()
                .field("input", "suggestion5", "suggestion6", "suggestion7")
                .field("weight", 5)
                .startObject("contexts")
                .array("loc1", "ezs42e44yx96")
                .array("loc2", "wh0n9447fwrc")
                .endObject()
                .endObject()
                .endArray()
                .endObject();
        ParsedDocument parsedDocument = defaultMapper.parse("test", "type1", "1", builder.bytes());
        IndexableField[] fields = parsedDocument.rootDoc().getFields(completionFieldType.name());
        assertContextSuggestFields(fields, 3);
    }

    public void testParsingQueryContextBasic() throws Exception {
        XContentBuilder builder = jsonBuilder().value("ezs42e44yx96");
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(createParseContext(parser));
        assertThat(internalQueryContexts.size(), equalTo(1 + 8));
        Collection<String> locations = new ArrayList<>();
        locations.add("ezs42e");
        addNeighbors("ezs42e", GeoContextMapping.DEFAULT_PRECISION, locations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            assertThat(internalQueryContext.context, isIn(locations));
            assertThat(internalQueryContext.boost, equalTo(1));
            assertThat(internalQueryContext.isPrefix, equalTo(false));
        }
    }

    public void testParsingQueryContextGeoPoint() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("lat", 23.654242)
                .field("lon", 90.047153)
                .endObject();
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(createParseContext(parser));
        assertThat(internalQueryContexts.size(), equalTo(1 + 8));
        Collection<String> locations = new ArrayList<>();
        locations.add("wh0n94");
        addNeighbors("wh0n94", GeoContextMapping.DEFAULT_PRECISION, locations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            assertThat(internalQueryContext.context, isIn(locations));
            assertThat(internalQueryContext.boost, equalTo(1));
            assertThat(internalQueryContext.isPrefix, equalTo(false));
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
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(createParseContext(parser));
        assertThat(internalQueryContexts.size(), equalTo(1 + 1 + 8 + 1 + 8 + 1 + 8));
        Collection<String> locations = new ArrayList<>();
        locations.add("wh0n94");
        locations.add("w");
        addNeighbors("w", 1, locations);
        locations.add("wh");
        addNeighbors("wh", 2, locations);
        locations.add("wh0");
        addNeighbors("wh0", 3, locations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            assertThat(internalQueryContext.context, isIn(locations));
            assertThat(internalQueryContext.boost, equalTo(10));
            assertThat(internalQueryContext.isPrefix, equalTo(internalQueryContext.context.length() < GeoContextMapping.DEFAULT_PRECISION));
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
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(createParseContext(parser));
        assertThat(internalQueryContexts.size(), equalTo(1 + 1 + 8 + 1 + 8 + 1 + 8 + 1 + 1 + 8));
        Collection<String> firstLocations = new ArrayList<>();
        firstLocations.add("wh0n94");
        firstLocations.add("w");
        addNeighbors("w", 1, firstLocations);
        firstLocations.add("wh");
        addNeighbors("wh", 2, firstLocations);
        firstLocations.add("wh0");
        addNeighbors("wh0", 3, firstLocations);
        Collection<String> secondLocations = new ArrayList<>();
        secondLocations.add("w5cx04");
        secondLocations.add("w5cx0");
        addNeighbors("w5cx0", 5, secondLocations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            if (firstLocations.contains(internalQueryContext.context)) {
                assertThat(internalQueryContext.boost, equalTo(10));
            } else if (secondLocations.contains(internalQueryContext.context)) {
                assertThat(internalQueryContext.boost, equalTo(2));
            } else {
                fail(internalQueryContext.context + " was not expected");
            }
            assertThat(internalQueryContext.isPrefix, equalTo(internalQueryContext.context.length() < GeoContextMapping.DEFAULT_PRECISION));
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
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
        GeoContextMapping mapping = ContextBuilder.geo("geo").build();
        List<ContextMapping.InternalQueryContext> internalQueryContexts = mapping.parseQueryContext(createParseContext(parser));
        assertThat(internalQueryContexts.size(), equalTo(1 + 1 + 8 + 1 + 8 + 1 + 8));
        Collection<String> firstLocations = new ArrayList<>();
        firstLocations.add("wh0n94");
        firstLocations.add("w");
        addNeighbors("w", 1, firstLocations);
        firstLocations.add("wh");
        addNeighbors("wh", 2, firstLocations);
        Collection<String> secondLocations = new ArrayList<>();
        secondLocations.add("w5cx04");
        addNeighbors("w5cx04", 6, secondLocations);
        for (ContextMapping.InternalQueryContext internalQueryContext : internalQueryContexts) {
            if (firstLocations.contains(internalQueryContext.context)) {
                assertThat(internalQueryContext.boost, equalTo(10));
            } else if (secondLocations.contains(internalQueryContext.context)) {
                assertThat(internalQueryContext.boost, equalTo(1));
            } else {
                fail(internalQueryContext.context + " was not expected");
            }
            assertThat(internalQueryContext.isPrefix, equalTo(internalQueryContext.context.length() < GeoContextMapping.DEFAULT_PRECISION));
        }
    }

    private static QueryParseContext createParseContext(XContentParser parser) {
        return new QueryParseContext(new IndicesQueriesRegistry(), parser, ParseFieldMatcher.STRICT);
    };
}
