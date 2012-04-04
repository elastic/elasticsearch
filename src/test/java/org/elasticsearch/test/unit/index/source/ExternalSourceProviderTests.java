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

package org.elasticsearch.test.unit.index.source;

import org.apache.lucene.document.Document;
import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.testng.annotations.Test;

import static org.elasticsearch.test.unit.index.mapper.MapperTests.newAnalysisService;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class ExternalSourceProviderTests {

    public static MapperService newMapperService() {
        return new MapperService(new Index("test"), ImmutableSettings.Builder.EMPTY_SETTINGS, new Environment(), newAnalysisService(), new TestExternalSourceProvider());
    }

    @Test
    public void testExternalSource() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("my_type")
                .startObject("_source")
                .endObject()
                .endObject().endObject().string();

        MapperService mapperService = newMapperService();
        mapperService.add("my_type", mapping);
        DocumentMapper documentMapper = mapperService.documentMapper("my_type");
        ParsedDocument doc = documentMapper.parse("my_type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field", "value")
                .endObject().copiedBytes());

        assertThat(doc.docs().size(), equalTo(1));
        Document document = doc.docs().get(0);
        assertThat(new String(document.getBinaryValue("_source")), equalTo("{\"dehydrated\":true}"));
        BytesHolder source = documentMapper.sourceMapper().nativeValue(document.getFieldable("_source"));
        BytesHolder rehydratedSource = mapperService.sourceProvider().rehydrateSource("type", "1", source.bytes(), source.offset(), source.length());
        String rehydratedSourceString = new String(rehydratedSource.bytes(), rehydratedSource.offset(), rehydratedSource.length());
        assertThat(rehydratedSourceString, equalTo("--type--1--{\"dehydrated\":true}--"));
    }

}
