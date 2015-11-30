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

package org.elasticsearch.plugin.indexbysearch;

import static org.elasticsearch.index.VersionType.EXTERNAL;
import static org.elasticsearch.index.VersionType.INTERNAL;

import org.elasticsearch.action.indexbysearch.IndexBySearchRequestBuilder;

public class IndexBySearchVersionTests extends IndexBySearchTestCase {
    // NOCOMMIT add tests for not clobbering versions on update

    public void testExternalVersioning() throws Exception {
        indexRandom(true, client().prepareIndex("test", "source", "test")
                .setVersionType(EXTERNAL)
                .setVersion(4).setSource("foo", "bar"));

        assertEquals(4, client().prepareGet("test", "source", "test").get().getVersion());

        // If the copy request
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.index().setIndex("test").setType("dest_internal").setVersionType(INTERNAL);
        assertResponse(copy.get(), 1, 0);
        refresh();
        assertEquals(1, client().prepareGet("test", "dest_internal", "test").get().getVersion());

        copy = newIndexBySearch();
        copy.index().setIndex("test").setType("dest_external").setVersionType(EXTERNAL);
        assertResponse(copy.get(), 1, 0);
        refresh();
        assertEquals(4, client().prepareGet("test", "dest_external", "test").get().getVersion());
    }
}
