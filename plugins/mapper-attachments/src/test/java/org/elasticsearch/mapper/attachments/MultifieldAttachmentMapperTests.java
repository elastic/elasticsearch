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

package org.elasticsearch.mapper.attachments;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.TextFieldMapper;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 *
 */
public class MultifieldAttachmentMapperTests extends AttachmentUnitTestCase {

    private DocumentMapperParser mapperParser;

    @Before
    public void setupMapperParser() throws Exception {
        mapperParser = MapperTestUtils.newMapperService(createTempDir(), Settings.EMPTY, getIndicesModuleWithRegisteredAttachmentMapper()).documentMapperParser();

    }

    public void testSimpleMappings() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/multifield/multifield-mapping.json");
        DocumentMapper docMapper = mapperParser.parse("person", new CompressedXContent(mapping));


        assertThat(docMapper.mappers().getMapper("file.content"), instanceOf(TextFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.content.suggest"), instanceOf(TextFieldMapper.class));

        assertThat(docMapper.mappers().getMapper("file.date"), instanceOf(DateFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.date.string"), instanceOf(TextFieldMapper.class));

        assertThat(docMapper.mappers().getMapper("file.title"), instanceOf(TextFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.title.suggest"), instanceOf(TextFieldMapper.class));

        assertThat(docMapper.mappers().getMapper("file.name"), instanceOf(TextFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.name.suggest"), instanceOf(TextFieldMapper.class));

        assertThat(docMapper.mappers().getMapper("file.author"), instanceOf(TextFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.author.suggest"), instanceOf(TextFieldMapper.class));

        assertThat(docMapper.mappers().getMapper("file.keywords"), instanceOf(TextFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.keywords.suggest"), instanceOf(TextFieldMapper.class));

        assertThat(docMapper.mappers().getMapper("file.content_type"), instanceOf(TextFieldMapper.class));
        assertThat(docMapper.mappers().getMapper("file.content_type.suggest"), instanceOf(TextFieldMapper.class));
    }

    public void testExternalValues() throws Exception {
        String originalText = "This is an elasticsearch mapper attachment test.";
        String forcedName = "dummyname.txt";

        String bytes = Base64.getEncoder().encodeToString(originalText.getBytes(StandardCharsets.ISO_8859_1));

        MapperService mapperService = MapperTestUtils.newMapperService(createTempDir(), Settings.EMPTY, getIndicesModuleWithRegisteredAttachmentMapper());

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/multifield/multifield-mapping.json");

        DocumentMapper documentMapper = mapperService.documentMapperParser().parse("person", new CompressedXContent(mapping));

        ParsedDocument doc = documentMapper.parse("person", "person", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .field("file", bytes)
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("file.content"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content").stringValue(), is(originalText + "\n"));

        assertThat(doc.rootDoc().getField("file.content_type"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_type").stringValue(), startsWith("text/plain;"));
        assertThat(doc.rootDoc().getField("file.content_type.suggest"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_type.suggest").stringValue(), startsWith("text/plain;"));
        assertThat(doc.rootDoc().getField("file.content_length"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_length").numericValue().intValue(), is(originalText.length()));

        assertThat(doc.rootDoc().getField("file.content.suggest"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content.suggest").stringValue(), is(originalText + "\n"));

        // Let's force some values
        doc = documentMapper.parse("person", "person", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("file")
                        .field("_content", bytes)
                        .field("_name", forcedName)
                    .endObject()
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("file.content"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content").stringValue(), is(originalText + "\n"));

        assertThat(doc.rootDoc().getField("file.content_type"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_type").stringValue(), startsWith("text/plain;"));
        assertThat(doc.rootDoc().getField("file.content_type.suggest"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_type.suggest").stringValue(), startsWith("text/plain;"));
        assertThat(doc.rootDoc().getField("file.content_length"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_length").numericValue().intValue(), is(originalText.length()));

        assertThat(doc.rootDoc().getField("file.content.suggest"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content.suggest").stringValue(), is(originalText + "\n"));

        assertThat(doc.rootDoc().getField("file.name"), notNullValue());
        assertThat(doc.rootDoc().getField("file.name").stringValue(), is(forcedName));
        // In mapping we have default store:false
        assertThat(doc.rootDoc().getField("file.name").fieldType().stored(), is(false));
        assertThat(doc.rootDoc().getField("file.name.suggest"), notNullValue());
        assertThat(doc.rootDoc().getField("file.name.suggest").stringValue(), is(forcedName));
        // In mapping we set store:true for suggest subfield
        assertThat(doc.rootDoc().getField("file.name.suggest").fieldType().stored(), is(true));
    }

    public void testAllExternalValues() throws Exception {
        String originalText = "This is an elasticsearch mapper attachment test.";
        String forcedName = randomAsciiOfLength(20);
        String forcedLanguage = randomAsciiOfLength(20);
        String forcedContentType = randomAsciiOfLength(20);

        String bytes = Base64.getEncoder().encodeToString(originalText.getBytes(StandardCharsets.ISO_8859_1));

        MapperService mapperService = MapperTestUtils.newMapperService(createTempDir(),
            Settings.builder().put(AttachmentMapper.INDEX_ATTACHMENT_DETECT_LANGUAGE_SETTING.getKey(), true).build(),
            getIndicesModuleWithRegisteredAttachmentMapper());

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/multifield/multifield-mapping.json");

        DocumentMapper documentMapper = mapperService.documentMapperParser().parse("person", new CompressedXContent(mapping));

        ParsedDocument doc = documentMapper.parse("person", "person", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("file")
                        .field("_content", bytes)
                        .field("_name", forcedName)
                        .field("_language", forcedLanguage)
                        .field("_content_type", forcedContentType)
                    .endObject()
                .endObject()
                .bytes());

        // Note that we don't support forcing values for _title and _keywords

        assertThat(doc.rootDoc().getField("file.content"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content").stringValue(), is(originalText + "\n"));

        assertThat(doc.rootDoc().getField("file.name"), notNullValue());
        assertThat(doc.rootDoc().getField("file.name").stringValue(), is(forcedName));
        assertThat(doc.rootDoc().getField("file.language"), notNullValue());
        assertThat(doc.rootDoc().getField("file.language").stringValue(), is(forcedLanguage));
        assertThat(doc.rootDoc().getField("file.content_type"), notNullValue());
        assertThat(doc.rootDoc().getField("file.content_type").stringValue(), is(forcedContentType));
    }
}
