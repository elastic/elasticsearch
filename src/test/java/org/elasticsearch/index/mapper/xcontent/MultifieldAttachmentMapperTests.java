/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.xcontent;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.attachment.AttachmentMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
@Test
public class MultifieldAttachmentMapperTests {

    private DocumentMapperParser mapperParser;

    @BeforeClass
    public void setupMapperParser() {
        mapperParser = new DocumentMapperParser(new Index("test"), new AnalysisService(new Index("test")), null, null);
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());
    }

    @Test
    public void testSimpleMappings() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/multifield-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);


        assertThat(docMapper.mappers().fullName("file").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("file.suggest").mapper(), instanceOf(StringFieldMapper.class));

        assertThat(docMapper.mappers().fullName("file.date").mapper(), instanceOf(DateFieldMapper.class));
        assertThat(docMapper.mappers().fullName("file.date.string").mapper(), instanceOf(StringFieldMapper.class));

        assertThat(docMapper.mappers().fullName("file.title").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("file.title.suggest").mapper(), instanceOf(StringFieldMapper.class));

        assertThat(docMapper.mappers().fullName("file.name").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("file.name.suggest").mapper(), instanceOf(StringFieldMapper.class));

        assertThat(docMapper.mappers().fullName("file.author").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("file.author.suggest").mapper(), instanceOf(StringFieldMapper.class));

        assertThat(docMapper.mappers().fullName("file.keywords").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("file.keywords.suggest").mapper(), instanceOf(StringFieldMapper.class));

        assertThat(docMapper.mappers().fullName("file.content_type").mapper(), instanceOf(StringFieldMapper.class));
        assertThat(docMapper.mappers().fullName("file.content_type.suggest").mapper(), instanceOf(StringFieldMapper.class));
    }
}
