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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class RankFeaturesFieldMapperTests extends ESSingleNodeTestCase {

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperExtrasPlugin.class);
    }

    public void testDefaults() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "rank_features").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                            .startObject("field")
                                .field("foo", 10)
                                .field("bar", 20)
                             .endObject()
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc1.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertThat(fields[0], Matchers.instanceOf(FeatureField.class));
        FeatureField featureField1 = (FeatureField) fields[0];
        assertThat(featureField1.stringValue(), Matchers.equalTo("foo"));
        FeatureField featureField2 = (FeatureField) fields[1];
        assertThat(featureField2.stringValue(), Matchers.equalTo("bar"));

        int freq1 = RankFeatureFieldMapperTests.getFrequency(featureField1.tokenStream(null, null));
        int freq2 = RankFeatureFieldMapperTests.getFrequency(featureField2.tokenStream(null, null));
        assertTrue(freq1 < freq2);
    }

    public void testRejectMultiValuedFields() throws MapperParsingException, IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "rank_features").endObject().startObject("foo")
                .startObject("properties").startObject("field").field("type", "rank_features").endObject().endObject()
                .endObject().endObject().endObject().endObject());

        DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", BytesReference
                        .bytes(XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("field")
                                    .field("foo", Arrays.asList(10, 20))
                                 .endObject()
                            .endObject()),
                        XContentType.JSON)));
        assertEquals("[rank_features] fields take hashes that map a feature to a strictly positive float, but got unexpected token " +
                "START_ARRAY", e.getCause().getMessage());

        e = expectThrows(MapperParsingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", BytesReference
                        .bytes(XContentFactory.jsonBuilder()
                                .startObject()
                                    .startArray("foo")
                                        .startObject()
                                            .startObject("field")
                                                .field("bar", 10)
                                            .endObject()
                                        .endObject()
                                        .startObject()
                                            .startObject("field")
                                                .field("bar", 20)
                                            .endObject()
                                        .endObject()
                                    .endArray()
                                .endObject()),
                        XContentType.JSON)));
        assertEquals("[rank_features] fields do not support indexing multiple values for the same rank feature [foo.field.bar] in " +
                "the same document", e.getCause().getMessage());
    }
}
