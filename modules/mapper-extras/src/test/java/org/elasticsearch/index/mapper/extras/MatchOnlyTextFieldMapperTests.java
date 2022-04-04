/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class MatchOnlyTextFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "value";
    }

    public final void testExists() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> { minimalMapping(b); }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(
            b -> { b.field("meta", Collections.singletonMap("format", "mysql.access")); },
            m -> assertEquals(Collections.singletonMap("format", "mysql.access"), m.fieldType().meta())
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "match_only_text");
    }

    @Override
    protected void minimalStoreMapping(XContentBuilder b) throws IOException {
        // 'store' is always true
        minimalMapping(b);
    }

    public void testDefaults() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals("1234", fields[0].stringValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertTrue(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());
    }

    public void testNullConfigValuesFail() throws MapperParsingException {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(fieldMapping(b -> b.field("type", "match_only_text").field("meta", (String) null)))
        );
        assertThat(e.getMessage(), containsString("[meta] on mapper [field] of type [match_only_text] must not have a [null] value"));
    }

    public void testSimpleMerge() throws IOException {
        XContentBuilder startingMapping = fieldMapping(b -> b.field("type", "match_only_text"));
        MapperService mapperService = createMapperService(startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(MatchOnlyTextFieldMapper.class));

        merge(mapperService, startingMapping);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(MatchOnlyTextFieldMapper.class));

        XContentBuilder newField = mapping(b -> {
            b.startObject("field").field("type", "match_only_text").startObject("meta").field("key", "value").endObject().endObject();
            b.startObject("other_field").field("type", "keyword").endObject();
        });
        merge(mapperService, newField);
        assertThat(mapperService.documentMapper().mappers().getMapper("field"), instanceOf(MatchOnlyTextFieldMapper.class));
        assertThat(mapperService.documentMapper().mappers().getMapper("other_field"), instanceOf(KeywordFieldMapper.class));
    }

    public void testDisabledSource() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        {
            mapping.startObject("properties");
            {
                mapping.startObject("foo");
                {
                    mapping.field("type", "match_only_text");
                }
                mapping.endObject();
            }
            mapping.endObject();

            mapping.startObject("_source");
            {
                mapping.field("enabled", false);
            }
            mapping.endObject();
        }
        mapping.endObject().endObject();

        MapperService mapperService = createMapperService(mapping);
        MappedFieldType ft = mapperService.fieldType("foo");
        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 4, 7));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ft.phraseQuery(ts, 0, true, context));
        assertThat(e.getMessage(), Matchers.containsString("cannot run positional queries since [_source] is disabled"));

        // Term queries are ok
        ft.termQuery("a", context); // no exception
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("We don't have a way to assert things here", true);
        return null;
    }

    @Override
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        assumeFalse("We don't have a way to assert things here", true);
    }
}
