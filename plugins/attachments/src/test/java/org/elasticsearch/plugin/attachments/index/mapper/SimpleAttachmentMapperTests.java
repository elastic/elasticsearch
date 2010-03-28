/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.plugin.attachments.index.mapper;

import org.apache.lucene.document.Document;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.json.JsonDocumentMapper;
import org.elasticsearch.index.mapper.json.JsonDocumentMapperParser;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.elasticsearch.util.io.Streams.*;
import static org.elasticsearch.util.json.JsonBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class SimpleAttachmentMapperTests {

    private JsonDocumentMapperParser mapperParser;

    @BeforeTest public void setupMapperParser() {
        mapperParser = new JsonDocumentMapperParser(new AnalysisService(new Index("test")));
        mapperParser.putTypeParser(JsonAttachmentMapper.JSON_TYPE, new JsonAttachmentTypeParser());
    }

    @Test public void testSimpleMappings() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/plugin/attachments/index/mapper/test-mapping.json");
        JsonDocumentMapper docMapper = (JsonDocumentMapper) mapperParser.parse(mapping);
        byte[] json = jsonBuilder().startObject().field("_id", 1).field("file", copyToBytesFromClasspath("/org/elasticsearch/plugin/attachments/index/mapper/testXHTML.html")).endObject().copiedBytes();

        Document doc = docMapper.parse(json).doc();

        assertThat(doc.get(docMapper.mappers().smartName("file.title").mapper().names().indexName()), equalTo("XHTML test document"));
        assertThat(doc.get(docMapper.mappers().smartName("file").mapper().names().indexName()), containsString("This document tests the ability of Apache Tika to extract content"));

        // re-parse it
        String builtMapping = docMapper.buildSource();
        docMapper = (JsonDocumentMapper) mapperParser.parse(builtMapping);

        json = jsonBuilder().startObject().field("_id", 1).field("file", copyToBytesFromClasspath("/org/elasticsearch/plugin/attachments/index/mapper/testXHTML.html")).endObject().copiedBytes();

        doc = docMapper.parse(json).doc();

        assertThat(doc.get(docMapper.mappers().smartName("file.title").mapper().names().indexName()), equalTo("XHTML test document"));
        assertThat(doc.get(docMapper.mappers().smartName("file").mapper().names().indexName()), containsString("This document tests the ability of Apache Tika to extract content"));
    }
}
