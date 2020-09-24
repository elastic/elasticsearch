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

package org.elasticsearch.index.mapper.murmur3;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapperTestCase2;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugin.mapper.MapperMurmur3Plugin;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class Murmur3FieldMapperTests extends FieldMapperTestCase2<Murmur3FieldMapper.Builder> {

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("value");
    }

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("analyzer", "similarity", "doc_values", "index");
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperMurmur3Plugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "murmur3");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) {
        // no parameters to configure
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.field("field", "value")));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertNotNull(fields);
        assertEquals(Arrays.toString(fields), 1, fields.length);
        IndexableField field = fields[0];
        assertEquals(IndexOptions.NONE, field.fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_NUMERIC, field.fieldType().docValuesType());
    }

    public void testDocValuesSettingNotAllowed() throws Exception {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "murmur3").field("doc_values", false)))
        );
        assertThat(e.getMessage(), containsString("Setting [doc_values] cannot be modified"));

        // even setting to the default is not allowed, the setting is invalid
        e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "murmur3").field("doc_values", true)))
        );
        assertThat(e.getMessage(), containsString("Setting [doc_values] cannot be modified"));
    }

    public void testIndexSettingNotAllowed() throws Exception {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "murmur3").field("index", "not_analyzed")))
        );
        assertThat(e.getMessage(), containsString("Setting [index] cannot be modified"));

        // even setting to the default is not allowed, the setting is invalid
        e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "murmur3").field("index", "no")))
        );
        assertThat(e.getMessage(), containsString("Setting [index] cannot be modified"));
    }

    @Override
    protected Murmur3FieldMapper.Builder newBuilder() {
        return new Murmur3FieldMapper.Builder("murmur");
    }
}
