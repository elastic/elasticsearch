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

import com.google.common.collect.Maps;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService.PutRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class MetaDataIndexTemplateServiceTests extends ESTestCase {
    @Test
    public void testIndexTemplateInvalidNumberOfShards() {
        PutRequest request = new PutRequest("test", "test_shards");
        request.template("test_shards*");

        Map<String, Object> map = Maps.newHashMap();
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

        Map<String, Object> map = Maps.newHashMap();
        map.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "0");
        request.settings(Settings.settingsBuilder().put(map).build());

        List<Throwable> throwables = putTemplate(request);
        assertEquals(throwables.size(), 1);
        assertThat(throwables.get(0), instanceOf(InvalidIndexTemplateException.class));
        assertThat(throwables.get(0).getMessage(), containsString("name must not contain a space"));
        assertThat(throwables.get(0).getMessage(), containsString("template must not start with '_'"));
        assertThat(throwables.get(0).getMessage(), containsString("index must have 1 or more primary shards"));
    }

    @Test
    public void testIndexTemplateWithAliasNameEqualToTemplatePattern() {
        PutRequest request = new PutRequest("api", "foobar_template");
        request.template("foobar");
        request.aliases(Collections.singleton(new Alias("foobar")));

        List<Throwable> errors = putTemplate(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(IllegalArgumentException.class));
        assertThat(errors.get(0).getMessage(), equalTo("Alias [foobar] cannot be the same as the template pattern [foobar]"));
    }

    private static List<Throwable> putTemplate(PutRequest request) {
        MetaDataCreateIndexService createIndexService = new MetaDataCreateIndexService(
                Settings.EMPTY,
                null,
                null,
                null,
                Version.CURRENT,
                null,
                Collections.EMPTY_SET,
                null
        );
        MetaDataIndexTemplateService service = new MetaDataIndexTemplateService(Settings.EMPTY, null, createIndexService, new AliasValidator(Settings.EMPTY));

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
