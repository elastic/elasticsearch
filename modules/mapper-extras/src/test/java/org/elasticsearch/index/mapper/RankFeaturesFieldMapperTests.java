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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.Plugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class RankFeaturesFieldMapperTests extends MapperTestCase {

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.startObject().field("foo", 10).field("bar", 20).endObject();
    }

    @Override
    protected void assertExistsQuery(MapperService mapperService) {
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> super.assertExistsQuery(mapperService));
        assertEquals("[rank_features] fields do not support [exists] queries", iae.getMessage());
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "rank_features");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) {
        // no parameters to configure
    }

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(source(this::writeField));

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
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("field").field("type", "rank_features").endObject();
            b.startObject("foo").startObject("properties");
            {
                b.startObject("field").field("type", "rank_features").endObject();
            }
            b.endObject().endObject();
        }));

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field").field("foo", Arrays.asList(10, 20)).endObject()))
        );
        assertEquals("[rank_features] fields take hashes that map a feature to a strictly positive float, but got unexpected token " +
                "START_ARRAY", e.getCause().getMessage());

        e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("foo");
            {
                b.startObject().startObject("field").field("bar", 10).endObject().endObject();
                b.startObject().startObject("field").field("bar", 20).endObject().endObject();
            }
            b.endArray();
        })));
        assertEquals("[rank_features] fields do not support indexing multiple values for the same rank feature [foo.field.bar] in " +
                "the same document", e.getCause().getMessage());
    }
}
