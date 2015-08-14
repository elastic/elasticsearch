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

package org.elasticsearch.action.admin.indices.template.put;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateFilter;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService.PutRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MetaDataIndexTemplateServiceTests extends ESTestCase {
    @Test
    public void testIndexTemplateInvalidNumberOfShards() throws IOException {
        MetaDataCreateIndexService createIndexService = new MetaDataCreateIndexService(
                Settings.EMPTY,
                null,
                null,
                null,
                null,
                null,
                Version.CURRENT,
                null,
                Sets.<IndexTemplateFilter>newHashSet(),
                null,
                null
        );
        MetaDataIndexTemplateService service = new MetaDataIndexTemplateService(Settings.EMPTY, null, createIndexService, null);

        PutRequest request = new PutRequest("test", "test_shards");
        request.template("test_shards*");

        Map<String, Object> map = Maps.newHashMap();
        map.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "0");
        request.settings(Settings.settingsBuilder().put(map).build());

        final List<Throwable> throwables = Lists.newArrayList();
        service.putTemplate(request, new MetaDataIndexTemplateService.PutListener() {
            @Override
            public void onResponse(MetaDataIndexTemplateService.PutResponse response) {

            }

            @Override
            public void onFailure(Throwable t) {
                throwables.add(t);
            }
        });
        assertEquals(throwables.size(), 1);
        assertTrue(throwables.get(0) instanceof InvalidIndexTemplateException);
        assertTrue(throwables.get(0).getMessage().contains("index must have 1 or more primary shards"));
    }
}
