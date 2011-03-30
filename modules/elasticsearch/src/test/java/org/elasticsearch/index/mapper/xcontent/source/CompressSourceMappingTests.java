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

package org.elasticsearch.index.mapper.xcontent.source;

import org.elasticsearch.common.compress.lzf.LZF;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.xcontent.MapperTests;
import org.elasticsearch.index.mapper.xcontent.XContentDocumentMapper;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class CompressSourceMappingTests {

    @Test public void testCompressDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").field("compress", false).endObject()
                .endObject().endObject().string();

        XContentDocumentMapper documentMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = documentMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject().copiedBytes());

        assertThat(LZF.isCompressed(doc.doc().getBinaryValue("_source")), equalTo(false));
    }

    @Test public void testCompressEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").field("compress", true).endObject()
                .endObject().endObject().string();

        XContentDocumentMapper documentMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = documentMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field1", "value1")
                .field("field2", "value2")
                .endObject().copiedBytes());

        assertThat(LZF.isCompressed(doc.doc().getBinaryValue("_source")), equalTo(true));
    }

    @Test public void testCompressThreshold() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").field("compress_threshold", "200b").endObject()
                .endObject().endObject().string();

        XContentDocumentMapper documentMapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = documentMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field1", "value1")
                .endObject().copiedBytes());

        assertThat(LZF.isCompressed(doc.doc().getBinaryValue("_source")), equalTo(false));

        doc = documentMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field1", "value1")
                .field("field2", "value2 xxxxxxxxxxxxxx yyyyyyyyyyyyyyyyyyy zzzzzzzzzzzzzzzzz")
                .field("field2", "value2 xxxxxxxxxxxxxx yyyyyyyyyyyyyyyyyyy zzzzzzzzzzzzzzzzz")
                .field("field2", "value2 xxxxxxxxxxxxxx yyyyyyyyyyyyyyyyyyy zzzzzzzzzzzzzzzzz")
                .field("field2", "value2 xxxxxxxxxxxxxx yyyyyyyyyyyyyyyyyyy zzzzzzzzzzzzzzzzz")
                .endObject().copiedBytes());

        assertThat(LZF.isCompressed(doc.doc().getBinaryValue("_source")), equalTo(true));
    }
}
