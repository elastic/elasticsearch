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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService.PutRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;

public class MetaDataIndexTemplateServiceTests extends ESTestCase {
    @Test
    public void testIndexTemplateInvalidNumberOfShards() {
        PutRequest request = new PutRequest("test", "test_shards");
        request.template("test_shards*");

        Map<String, Object> map = new HashMap<>();
        map.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "0");
        request.settings(Settings.settingsBuilder().put(map).build());

        List<Throwable> throwables = putTemplate(request);
        assertEquals(throwables.size(), 1);
        assertThat(throwables.get(0), instanceOf(InvalidIndexTemplateException.class));
        assertThat(throwables.get(0).getMessage(), containsString("index must have 1 or more primary shards"));
    }

    @Test
    public void testIndexTemplateValidationAccumulatesValidationErrors() {
        PutRequest request = new PutRequest("test", "putTemplate shards");
        request.template("_test_shards*");

        Map<String, Object> map = new HashMap<>();
        map.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "0");
        request.settings(Settings.settingsBuilder().put(map).build());

        List<Throwable> throwables = putTemplate(request);
        assertEquals(throwables.size(), 1);
        assertThat(throwables.get(0), instanceOf(InvalidIndexTemplateException.class));
        assertThat(throwables.get(0).getMessage(), containsString("name must not contain a space"));
        assertThat(throwables.get(0).getMessage(), containsString("template must not start with '_'"));
        assertThat(throwables.get(0).getMessage(), containsString("index must have 1 or more primary shards"));
    }

    private static List<Throwable> putTemplate(PutRequest request) {
        MetaDataCreateIndexService createIndexService = new MetaDataCreateIndexService(
                Settings.EMPTY,
                null,
                null,
                null,
                null,
                null,
                Version.CURRENT,
                null,
                new HashSet<>(),
                null,
                null
        );
        MetaDataIndexTemplateService service = new MetaDataIndexTemplateService(Settings.EMPTY, null, createIndexService, null);

        final List<Throwable> throwables = new ArrayList<>();
        service.putTemplate(request, new MetaDataIndexTemplateService.PutListener() {
            @Override
            public void onResponse(MetaDataIndexTemplateService.PutResponse response) {

            }

            @Override
            public void onFailure(Throwable t) {
                throwables.add(t);
            }
        });

        return throwables;
    }
}
