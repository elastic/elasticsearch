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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;

import static org.hamcrest.Matchers.endsWith;

public class MetaDataCreateIndexServiceTests extends ESTestCase {

    public void testValidateIndexName() throws Exception {

        validateIndexName("index?name", "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);

        validateIndexName("index#name", "must not contain '#'");

        validateIndexName("_indexname", "must not start with '_'");

        validateIndexName("INDEXNAME", "must be lowercase");

        validateIndexName("..", "must not be '.' or '..'");

    }

    private void validateIndexName(String indexName, String errorMessage) {
        try {
            getCreateIndexService().validateIndexName(indexName, ClusterState.builder(ClusterName.DEFAULT).build());
            fail("");
        } catch (InvalidIndexNameException e) {
            assertThat(e.getMessage(), endsWith(errorMessage));
        }
    }

    private MetaDataCreateIndexService getCreateIndexService() {
        return new MetaDataCreateIndexService(
            Settings.EMPTY,
            null,
            null,
            null,
            Version.CURRENT,
            null,
            new HashSet<IndexTemplateFilter>(),
            null);
    }
}
