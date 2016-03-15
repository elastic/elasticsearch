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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MetaDataTests extends ESTestCase {

    public void testIndexAndAliasWithSameName() {
        IndexMetaData.Builder builder = IndexMetaData.builder("index")
                .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(AliasMetaData.builder("index").build());
        try {
            MetaData.builder().put(builder).build();
            fail("expection should have been thrown");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("index and alias names need to be unique, but alias [index] and index [index] have the same name"));
        }
    }


    @Test
    public void testMetaDataTemplateUpgrade() throws Exception {
        MetaData metaData = MetaData.builder()
                .put(IndexTemplateMetaData.builder("t1").settings(
                        Settings.builder().put("index.translog.interval", 8000))).build();

        MetaData newMd = MetaData.addDefaultUnitsIfNeeded(Loggers.getLogger(MetaDataTests.class), metaData);

        assertThat(newMd.getTemplates().get("t1").getSettings().get("index.translog.interval"), is("8000ms"));
    }
}
