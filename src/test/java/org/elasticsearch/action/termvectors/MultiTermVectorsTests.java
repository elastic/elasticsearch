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

package org.elasticsearch.action.termvectors;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

public class MultiTermVectorsTests extends AbstractTermVectorsTests {

    @Test
    public void testDuelESLucene() throws Exception {
        AbstractTermVectorsTests.TestFieldSetting[] testFieldSettings = getFieldSettings();
        createIndexBasedOnFieldSettings("test", "alias", testFieldSettings);
        //we generate as many docs as many shards we have
        TestDoc[] testDocs = generateTestDocs("test", testFieldSettings);

        DirectoryReader directoryReader = indexDocsWithLucene(testDocs);
        AbstractTermVectorsTests.TestConfig[] testConfigs = generateTestConfigs(20, testDocs, testFieldSettings);

        MultiTermVectorsRequestBuilder requestBuilder = client().prepareMultiTermVectors();
        for (AbstractTermVectorsTests.TestConfig test : testConfigs) {
            requestBuilder.add(getRequestForConfig(test).request());
        }

        MultiTermVectorsItemResponse[] responseItems = requestBuilder.get().getResponses();

        for (int i = 0; i < testConfigs.length; i++) {
            TestConfig test = testConfigs[i];
            try {
                MultiTermVectorsItemResponse item = responseItems[i];
                if (test.expectedException != null) {
                    assertTrue(item.isFailed());
                    continue;
                } else if (item.isFailed()) {
                    fail(item.getFailure().getMessage());
                }
                Fields luceneTermVectors = getTermVectorsFromLucene(directoryReader, test.doc);
                validateResponse(item.getResponse(), luceneTermVectors, test);
            } catch (Throwable t) {
                throw new Exception("Test exception while running " + test.toString(), t);
            }
        }

    }

    @Test
    public void testMissingIndexThrowsMissingIndex() throws Exception {
        TermVectorsRequestBuilder requestBuilder = client().prepareTermVectors("testX", "typeX", Integer.toString(1));
        MultiTermVectorsRequestBuilder mtvBuilder = new MultiTermVectorsRequestBuilder(client());
        mtvBuilder.add(requestBuilder.request());
        MultiTermVectorsResponse response = mtvBuilder.execute().actionGet();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getFailure().getMessage(), equalTo("[" + response.getResponses()[0].getIndex() + "] missing"));
    }

    @Test
    public void testMultiTermVectorsWithVersion() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
                .setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1)));
        ensureGreen();

        MultiTermVectorsResponse response = client().prepareMultiTermVectors().add(indexOrAlias(), "type1", "1").get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(false));

        for (int i = 0; i < 3; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value" + i).get();
        }

        // Version from translog
        response = client().prepareMultiTermVectors()
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "1").selectedFields("field").version(Versions.MATCH_ANY))
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "1").selectedFields("field").version(1))
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "1").selectedFields("field").version(2))
                .get();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("1"));
        assertThat(response.getResponses()[0].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        checkTermTexts(response.getResponses()[0].getResponse().getFields().terms("field"), new String[]{"value1"});
        assertThat(response.getResponses()[1].getId(), equalTo("1"));
        assertThat(response.getResponses()[1].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getFailure(), nullValue());
        assertThat(response.getResponses()[1].getResponse().isExists(), equalTo(true));
        checkTermTexts(response.getResponses()[1].getResponse().getFields().terms("field"), new String[]{"value1"});
        assertThat(response.getResponses()[2].getFailure(), notNullValue());
        assertThat(response.getResponses()[2].getFailure().getId(), equalTo("1"));
        assertThat(response.getResponses()[2].getFailure().getMessage(), startsWith("VersionConflictEngineException"));

        //Version from Lucene index
        refresh();
        response = client().prepareMultiTermVectors()
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "1").selectedFields("field").version(Versions.MATCH_ANY).realtime(false))
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "1").selectedFields("field").version(1).realtime(false))
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "1").selectedFields("field").version(2).realtime(false))
                .get();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("1"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        checkTermTexts(response.getResponses()[0].getResponse().getFields().terms("field"), new String[]{"value1"});
        assertThat(response.getResponses()[1].getId(), equalTo("1"));
        assertThat(response.getResponses()[1].getFailure(), nullValue());
        assertThat(response.getResponses()[1].getResponse().isExists(), equalTo(true));
        checkTermTexts(response.getResponses()[1].getResponse().getFields().terms("field"), new String[]{"value1"});
        assertThat(response.getResponses()[2].getFailure(), notNullValue());
        assertThat(response.getResponses()[2].getFailure().getId(), equalTo("1"));
        assertThat(response.getResponses()[2].getFailure().getMessage(), startsWith("VersionConflictEngineException"));


        for (int i = 0; i < 3; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value" + i).get();
        }

        // Version from translog
        response = client().prepareMultiTermVectors()
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "2").selectedFields("field").version(Versions.MATCH_ANY))
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "2").selectedFields("field").version(1))
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "2").selectedFields("field").version(2))
                .get();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("2"));
        assertThat(response.getResponses()[0].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        checkTermTexts(response.getResponses()[0].getResponse().getFields().terms("field"), new String[]{"value2"});
        assertThat(response.getResponses()[1].getFailure(), notNullValue());
        assertThat(response.getResponses()[1].getFailure().getId(), equalTo("2"));
        assertThat(response.getResponses()[1].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getFailure().getMessage(), startsWith("VersionConflictEngineException"));
        assertThat(response.getResponses()[2].getId(), equalTo("2"));
        assertThat(response.getResponses()[2].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[2].getFailure(), nullValue());
        assertThat(response.getResponses()[2].getResponse().isExists(), equalTo(true));
        checkTermTexts(response.getResponses()[2].getResponse().getFields().terms("field"), new String[]{"value2"});


        //Version from Lucene index
        refresh();
        response = client().prepareMultiTermVectors()
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "2").selectedFields("field").version(Versions.MATCH_ANY))
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "2").selectedFields("field").version(1))
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "2").selectedFields("field").version(2))
                .get();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("2"));
        assertThat(response.getResponses()[0].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        checkTermTexts(response.getResponses()[0].getResponse().getFields().terms("field"), new String[]{"value2"});
        assertThat(response.getResponses()[1].getFailure(), notNullValue());
        assertThat(response.getResponses()[1].getFailure().getId(), equalTo("2"));
        assertThat(response.getResponses()[1].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getFailure().getMessage(), startsWith("VersionConflictEngineException"));
        assertThat(response.getResponses()[2].getId(), equalTo("2"));
        assertThat(response.getResponses()[2].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[2].getFailure(), nullValue());
        assertThat(response.getResponses()[2].getResponse().isExists(), equalTo(true));
        checkTermTexts(response.getResponses()[2].getResponse().getFields().terms("field"), new String[]{"value2"});
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }

    private void checkTermTexts(Terms terms, String[] expectedTexts) throws IOException {
        final TermsEnum termsEnum = terms.iterator();
        for (String expectedText : expectedTexts) {
            assertThat(termsEnum.next().utf8ToString(), equalTo(expectedText));
        }
    }
}
