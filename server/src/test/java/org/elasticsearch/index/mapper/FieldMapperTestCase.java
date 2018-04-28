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

import com.google.common.collect.Lists;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;

public abstract class FieldMapperTestCase extends ESSingleNodeTestCase {

    public static String fieldPath(final String field, String...fields){
        final List<String> fieldPath = Lists.newArrayList();
        fieldPath.add(field);
        fieldPath.addAll(Arrays.asList(fields));
        return fieldPath.stream()
            .collect(Collectors.joining("."));
    }

    protected static final String INDEX = "test";
    protected static final String TYPE = "type";

    protected IndexService indexService;
    protected DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex(INDEX);
        parser = indexService.mapperService().documentMapperParser();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    protected final DocumentMapper documentMapper() throws IOException {
        return this.documentMapper(this.mapping());
    }

    protected final DocumentMapper documentMapper(final String mapping) throws IOException {
        return parser.parse(TYPE, new CompressedXContent(mapping));
    }

    protected final void documentMapperFails(final String mapping,
                                             final Class<? extends Throwable> thrown,
                                             final String messageContains) throws IOException {
        Throwable e = expectThrows(thrown,
            () -> documentMapper(mapping)
        );
        log(e);
        assertThat(e.getMessage(), containsString(messageContains));
    }

    protected final DocumentMapper documentMapperFromIndexService() throws IOException {
        return this.indexService.mapperService().documentMapper(TYPE);
    }

    protected abstract String mapping() throws IOException;

    protected final List<ParseContext.Document> mappingParse(final BytesReference source) throws IOException {
        return this.mappingParse(this.documentMapper(), source);
    }

    private List<ParseContext.Document> mappingParse(final DocumentMapper mapper, final BytesReference source) {
        return mapper.parse(SourceToParse.source(INDEX, TYPE, "1",
            source,
            XContentType.JSON))
            .docs();
    }

    protected final void mappingParseFails(final BytesReference source,
                                           final Class<? extends Throwable> thrown,
                                           final String messageContains) throws IOException {
        this.mappingParseFails(this.documentMapper(), source, thrown, messageContains);
    }

    protected final void mappingParseFails(final DocumentMapper mapper,
                                           final BytesReference source,
                                           final Class<? extends Throwable> thrown,
                                           final String messageContains) throws IOException {
        Throwable e = expectThrows(thrown,
            () -> mappingParse(mapper, source)
        );
        log(e);
        assertThat(e.getMessage() + " " + e.getCause().getMessage(), containsString(messageContains));
    }

    protected void mergeMapping(final String mapping) throws IOException {
        this.mergeMapping(new CompressedXContent(mapping));
    }

    protected void mergeMapping(final CompressedXContent mapping) {
        this.indexService.mapperService().merge(TYPE,
            mapping,
            MapperService.MergeReason.MAPPING_UPDATE);
    }

    protected void mergeMappingFails(final String mapping,
                                     final Class<? extends Throwable> thrown,
                                     final String messageContains) throws IOException {
        Throwable e = expectThrows(thrown,
            () -> mergeMapping(mapping)
        );
        log(e);
        assertThat(e.getMessage() + " " + e.getCause().getMessage(), containsString(messageContains));
    }

    protected QueryShardContext queryShardContext() {
        return indexService.newQueryShardContext(
            randomInt(20), null, () -> {
            throw new UnsupportedOperationException();
        }, null);
    }

    protected void checkDocumentCount(final int expected, final List<ParseContext.Document> documents) {
        assertEquals(expected + " documents: " + documents, expected, documents.size());
    }

    protected MappedFieldType fieldType(final DocumentMapper mapper, final String field) {
        return mapper.mappers().getMapper(field).fieldType();
    }

    protected void checkField(final ParseContext.Document document, final String field, final String value) {
        final IndexableField[] fields = document.getFields(field);

        assertEquals("index fields=" + Arrays.toString(fields), 1, fields.length);
        assertEquals("IndexableField[0] name=" + field, field, fields[0].name());
        assertEquals("IndexableField[0] stringValue=" + field, value, fields[0].stringValue());
    }

    protected void checkFieldMapping(final DocumentMapper documentMapper,
                                     final String field,
                                     final String mapping) throws IOException {
        final FieldMapper mapper = documentMapper.mappers().getMapper(field);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        this.checkJsonEqual("field [" + field + "] mapping",
            mapping,
            Strings.toString(builder));
    }

    protected final void checkJsonEqual(final String message, final String expected, final String actual) {
        Assert.assertEquals(message, pretty(expected), pretty(actual));
    }

    private String pretty(final String json) {
        return json.replace("'", "\\\""); // TODO really pretty
    }

    private void log(Throwable t)
    {
        //t.printStackTrace();
    }
}
