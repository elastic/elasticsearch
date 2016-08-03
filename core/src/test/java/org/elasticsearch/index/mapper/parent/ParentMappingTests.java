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
package org.elasticsearch.index.mapper.parent;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ParentMappingTests extends ESSingleNodeTestCase {

    public void testParentSetInDocNotAllowed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        try {
            docMapper.parse(SourceToParse.source("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject().field("_parent", "1122").endObject().bytes()));
            fail("Expected failure to parse metadata field");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Field [_parent] is a metadata field and cannot be added inside a document"));
        }
    }

    public void testJoinFieldSet() throws Exception {
        String parentMapping = XContentFactory.jsonBuilder().startObject().startObject("parent_type")
                .endObject().endObject().string();
        String childMapping = XContentFactory.jsonBuilder().startObject().startObject("child_type")
                .startObject("_parent").field("type", "parent_type").endObject()
                .endObject().endObject().string();
        IndexService indexService = createIndex("test");
        indexService.mapperService().merge("parent_type", new CompressedXContent(parentMapping), MergeReason.MAPPING_UPDATE, false);
        indexService.mapperService().merge("child_type", new CompressedXContent(childMapping), MergeReason.MAPPING_UPDATE, false);

        // Indexing parent doc:
        DocumentMapper parentDocMapper = indexService.mapperService().documentMapper("parent_type");
        ParsedDocument doc = parentDocMapper.parse(SourceToParse.source("test", "parent_type", "1122", new BytesArray("{}")));
        assertEquals(1, getNumberOfFieldWithParentPrefix(doc.rootDoc()));
        assertEquals("1122", doc.rootDoc().getBinaryValue("_parent#parent_type").utf8ToString());

        // Indexing child doc:
        DocumentMapper childDocMapper = indexService.mapperService().documentMapper("child_type");
        doc = childDocMapper.parse(SourceToParse.source("test", "child_type", "1", new BytesArray("{}")).parent("1122"));

        assertEquals(1, getNumberOfFieldWithParentPrefix(doc.rootDoc()));
        assertEquals("1122", doc.rootDoc().getBinaryValue("_parent#parent_type").utf8ToString());
    }

    public void testJoinFieldNotSet() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        ParsedDocument doc = docMapper.parse(SourceToParse.source("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("x_field", "x_value")
                .endObject()
                .bytes()));
        assertEquals(0, getNumberOfFieldWithParentPrefix(doc.rootDoc()));
    }

    public void testNoParentNullFieldCreatedIfNoParentSpecified() throws Exception {
        Index index = new Index("_index", "testUUID");
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, Settings.EMPTY);
        AnalysisService analysisService = new AnalysisService(indexSettings, Collections.emptyMap(), Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptyMap());
        SimilarityService similarityService = new SimilarityService(indexSettings, Collections.emptyMap());
        MapperService mapperService = new MapperService(indexSettings, analysisService, similarityService,
            new IndicesModule(emptyList()).getMapperRegistry(), () -> null);
        XContentBuilder mappingSource = jsonBuilder().startObject().startObject("some_type")
            .startObject("properties")
            .endObject()
            .endObject().endObject();
        mapperService.merge("some_type", new CompressedXContent(mappingSource.string()), MergeReason.MAPPING_UPDATE, false);
        Set<String> allFields = new HashSet<>(mapperService.simpleMatchToIndexNames("*"));
        assertTrue(allFields.contains("_parent"));
        assertFalse(allFields.contains("_parent#null"));
    }

    private static int getNumberOfFieldWithParentPrefix(ParseContext.Document doc) {
        int numFieldWithParentPrefix = 0;
        for (IndexableField field : doc) {
            if (field.name().startsWith("_parent")) {
                numFieldWithParentPrefix++;
            }
        }
        return numFieldWithParentPrefix;
    }
}
