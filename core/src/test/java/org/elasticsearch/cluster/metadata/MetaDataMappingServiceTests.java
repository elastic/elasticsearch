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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MetaDataMappingServiceTests extends ESSingleNodeTestCase {

    // Tests _parent meta field logic, because part of the validation is in MetaDataMappingService
    public void testAddChildTypePointingToAlreadyExistingType() throws Exception {
        createIndex("test", Settings.EMPTY, "type", "field", "type=keyword");

        // Shouldn't be able the add the _parent field pointing to an already existing type, which isn't a parent type
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource("_parent", "type=type")
                .get());
        assertThat(e.getMessage(),
                equalTo("can't add a _parent field that points to an already existing type, that isn't already a parent"));
    }

    // Tests _parent meta field logic, because part of the validation is in MetaDataMappingService
    public void testAddExtraChildTypePointingToAlreadyParentExistingType() throws Exception {
        IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("parent")
                .addMapping("child1", "_parent", "type=parent")
        );

        // adding the extra child type that points to an already existing parent type is allowed:
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child2")
                .setSource("_parent", "type=parent")
                .get();

        DocumentMapper documentMapper = indexService.mapperService().documentMapper("child2");
        assertThat(documentMapper.parentFieldMapper().type(), equalTo("parent"));
        assertThat(documentMapper.parentFieldMapper().active(), is(true));
    }

}
