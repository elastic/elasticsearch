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

package org.elasticsearch.index.mapper.xcontent.multifield.merge;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.xcontent.XContentDocumentMapper;
import org.elasticsearch.index.mapper.xcontent.XContentDocumentMapperParser;
import org.testng.annotations.Test;

import static org.elasticsearch.common.io.Streams.*;
import static org.elasticsearch.index.mapper.DocumentMapper.MergeFlags.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class JavaMultiFieldMergeTests {

    @Test public void testMergeMultiField() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/multifield/merge/test-mapping1.json");
        XContentDocumentMapper docMapper = (XContentDocumentMapper) new XContentDocumentMapperParser(new AnalysisService(new Index("test"))).parse(mapping);

        assertThat(docMapper.mappers().fullName("name").mapper().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name.indexed"), nullValue());

        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/multifield/merge/test-data.json");
        Document doc = docMapper.parse(json).doc();
        Field f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, nullValue());


        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/multifield/merge/test-mapping2.json");
        XContentDocumentMapper docMapper2 = (XContentDocumentMapper) new XContentDocumentMapperParser(new AnalysisService(new Index("test"))).parse(mapping);

        docMapper.merge(docMapper2, mergeFlags().simulate(true));

        docMapper.merge(docMapper2, mergeFlags().simulate(false));

        assertThat(docMapper.mappers().name("name").mapper().indexed(), equalTo(true));

        assertThat(docMapper.mappers().fullName("name").mapper().indexed(), equalTo(true));
        assertThat(docMapper.mappers().fullName("name.indexed").mapper(), notNullValue());

        json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/multifield/merge/test-data.json");
        doc = docMapper.parse(json).doc();
        f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, notNullValue());
    }
}
