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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.*;

/**
 * Test for https://github.com/elasticsearch/elasticsearch-mapper-attachments/issues/38
 */
public class MetadataMapperTests extends AttachmentUnitTestCase {

    protected void checkMeta(String filename, Settings otherSettings, Long expectedDate, Long expectedLength) throws IOException {
        Settings settings = Settings.builder()
                                             .put(this.testSettings)
                                             .put(otherSettings)
                                             .build();
        DocumentMapperParser mapperParser = MapperTestUtils.newMapperService(createTempDir(), settings, getIndicesModuleWithRegisteredAttachmentMapper()).documentMapperParser();

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/metadata/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/" + filename);

        BytesReference json = jsonBuilder()
                .startObject()
                    .startObject("file")
                        .field("_name", filename)
                        .field("_content", html)
                    .endObject()
                .endObject().bytes();

        ParseContext.Document doc =  docMapper.parse("person", "person", "1", json).rootDoc();
        assertThat(doc.get(docMapper.mappers().getMapper("file.content").fieldType().names().indexName()), containsString("World"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.name").fieldType().names().indexName()), equalTo(filename));
        if (expectedDate == null) {
            assertThat(doc.getField(docMapper.mappers().getMapper("file.date").fieldType().names().indexName()), nullValue());
        } else {
            assertThat(doc.getField(docMapper.mappers().getMapper("file.date").fieldType().names().indexName()).numericValue().longValue(), is(expectedDate));
        }
        assertThat(doc.get(docMapper.mappers().getMapper("file.title").fieldType().names().indexName()), equalTo("Hello"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.author").fieldType().names().indexName()), equalTo("kimchy"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.keywords").fieldType().names().indexName()), equalTo("elasticsearch,cool,bonsai"));
        assertThat(doc.get(docMapper.mappers().getMapper("file.content_type").fieldType().names().indexName()), startsWith("text/html;"));
        if (expectedLength == null) {
          assertNull(doc.getField(docMapper.mappers().getMapper("file.content_length").fieldType().names().indexName()).numericValue().longValue());
        } else {
          assertThat(doc.getField(docMapper.mappers().getMapper("file.content_length").fieldType().names().indexName()).numericValue().longValue(), greaterThan(0L));
        }
    }

    public void testIgnoreWithoutDate() throws Exception {
        checkMeta("htmlWithoutDateMeta.html", Settings.builder().build(), null, 300L);
    }

    public void testIgnoreWithEmptyDate() throws Exception {
        checkMeta("htmlWithEmptyDateMeta.html", Settings.builder().build(), null, 334L);
    }

    public void testIgnoreWithCorrectDate() throws Exception {
        checkMeta("htmlWithValidDateMeta.html", Settings.builder().build(), 1354233600000L, 344L);
    }

    public void testWithoutDate() throws Exception {
        checkMeta("htmlWithoutDateMeta.html", Settings.builder().put("index.mapping.attachment.ignore_errors", false).build(), null, 300L);
    }

    public void testWithEmptyDate() throws Exception {
        try {
            checkMeta("htmlWithEmptyDateMeta.html", Settings.builder().put("index.mapping.attachment.ignore_errors", false).build(), null, null);
        } catch (MapperParsingException expected) {
            assertTrue(expected.getMessage().contains("failed to parse"));
        }
    }

    public void testWithCorrectDate() throws Exception {
        checkMeta("htmlWithValidDateMeta.html", Settings.builder().put("index.mapping.attachment.ignore_errors", false).build(), 1354233600000L, 344L);
    }
}
