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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

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

    public void testResolveIndexRouting() {
        IndexMetaData.Builder builder = IndexMetaData.builder("index")
                .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(AliasMetaData.builder("alias0").build())
                .putAlias(AliasMetaData.builder("alias1").routing("1").build())
                .putAlias(AliasMetaData.builder("alias2").routing("1,2").build());
        MetaData metaData = MetaData.builder().put(builder).build();

        // no alias, no index
        assertEquals(metaData.resolveIndexRouting(null, null, null), null);
        assertEquals(metaData.resolveIndexRouting(null, "0", null), "0");
        assertEquals(metaData.resolveIndexRouting("32", "0", null), "0");
        assertEquals(metaData.resolveIndexRouting("32", null, null), "32");

        // index, no alias
        assertEquals(metaData.resolveIndexRouting("32", "0", "index"), "0");
        assertEquals(metaData.resolveIndexRouting("32", null, "index"), "32");
        assertEquals(metaData.resolveIndexRouting(null, null, "index"), null);
        assertEquals(metaData.resolveIndexRouting(null, "0", "index"), "0");

        // alias with no index routing
        assertEquals(metaData.resolveIndexRouting(null, null, "alias0"), null);
        assertEquals(metaData.resolveIndexRouting(null, "0", "alias0"), "0");
        assertEquals(metaData.resolveIndexRouting("32", null, "alias0"), "32");
        assertEquals(metaData.resolveIndexRouting("32", "0", "alias0"), "0");

        // alias with index routing.
        assertEquals(metaData.resolveIndexRouting(null, null, "alias1"), "1");
        assertEquals(metaData.resolveIndexRouting("32", null, "alias1"), "1");
        assertEquals(metaData.resolveIndexRouting("32", "1", "alias1"), "1");
        try {
            metaData.resolveIndexRouting(null, "0", "alias1");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Alias [alias1] has index routing associated with it [1], and was provided with routing value [0], rejecting operation"));
        }

        try {
            metaData.resolveIndexRouting("32", "0", "alias1");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Alias [alias1] has index routing associated with it [1], and was provided with routing value [0], rejecting operation"));
        }

        // alias with invalid index routing.
        try {
            metaData.resolveIndexRouting(null, null, "alias2");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation"));
        }

        try {
            metaData.resolveIndexRouting(null, "1", "alias2");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation"));
        }

        try {
            metaData.resolveIndexRouting("32", null, "alias2");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation"));
        }
    }
}
