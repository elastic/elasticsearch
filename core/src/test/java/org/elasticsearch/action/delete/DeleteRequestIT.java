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

package org.elasticsearch.action.delete;

import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class DeleteRequestIT extends ESIntegTestCase {

    public void testDeleteDoesNotCreateMapping() {
        try {
            client().prepareDelete("test", "test", "test").get();
            fail("Expected to fail with IndexNotFoundException");
        } catch (IndexNotFoundException expected) {
            // ignore
        }
        assertThat(client().admin().cluster().prepareState().all().get().getState().getMetaData().getIndices().size(), equalTo(0));
    }

    public void testDeleteWithExternalVersioningCreatesMapping() {
        client().prepareDelete("test", "test", "test")
            .setVersion(randomIntBetween(0, 1000))
            .setVersionType(randomFrom(VersionType.EXTERNAL, VersionType.EXTERNAL_GTE))
            .get();
        assertThat(client().admin().cluster().prepareState().all().get().getState().getMetaData().index("test"), notNullValue());
    }
}
