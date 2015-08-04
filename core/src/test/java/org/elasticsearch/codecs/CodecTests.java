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

package org.elasticsearch.codecs;

import org.apache.lucene.codecs.Codec;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.Assert;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class CodecTests extends ESSingleNodeTestCase {

    public void testAcceptPostingsFormat() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("postings_format", Codec.getDefault().postingsFormat().getName()).endObject().endObject()
                .endObject().endObject().string();
        int i = 0;
        for (Version v : VersionUtils.allVersions()) {
            IndexService indexService = createIndex("test-" + i++, Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, v).build());
            DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
            try {
                parser.parse(mapping);
                if (v.onOrAfter(Version.V_2_0_0_beta1)) {
                    fail("Elasticsearch 2.0 should not support custom postings formats");
                }
            } catch (MapperParsingException e) {
                if (v.before(Version.V_2_0_0_beta1)) {
                    // Elasticsearch 1.x should ignore custom postings formats
                    throw e;
                }
                Assert.assertThat(e.getMessage(), containsString("unsupported parameters:  [postings_format"));
            }
        }
    }

    public void testAcceptDocValuesFormat() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("doc_values_format", Codec.getDefault().docValuesFormat().getName()).endObject().endObject()
                .endObject().endObject().string();
        int i = 0;
        for (Version v : VersionUtils.allVersions()) {
            IndexService indexService = createIndex("test-" + i++, Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, v).build());
            DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
            try {
                parser.parse(mapping);
                if (v.onOrAfter(Version.V_2_0_0_beta1)) {
                    fail("Elasticsearch 2.0 should not support custom postings formats");
                }
            } catch (MapperParsingException e) {
                if (v.before(Version.V_2_0_0_beta1)) {
                    // Elasticsearch 1.x should ignore custom postings formats
                    throw e;
                }
                Assert.assertThat(e.getMessage(), containsString("unsupported parameters:  [doc_values_format"));
            }
        }
    }

}
