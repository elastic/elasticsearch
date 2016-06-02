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
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService.PutRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class MetaDataIndexTemplateServiceTests extends ESSingleNodeTestCase {
    public void testIndexTemplateInvalidNumberOfShards() {
        PutRequest request = new PutRequest("test", "test_shards");
        request.template("test_shards*");

        Map<String, Object> map = new HashMap<>();
        map.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "0");
        request.settings(Settings.builder().put(map).build());

        List<Throwable> throwables = putTemplate(request);
        assertEquals(throwables.size(), 1);
        assertThat(throwables.get(0), instanceOf(InvalidIndexTemplateException.class));
        assertThat(throwables.get(0).getMessage(), containsString("index must have 1 or more primary shards"));
    }

    public void testIndexTemplateValidationAccumulatesValidationErrors() {
        PutRequest request = new PutRequest("test", "putTemplate shards");
        request.template("_test_shards*");

        Map<String, Object> map = new HashMap<>();
        map.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "0");
        request.settings(Settings.builder().put(map).build());

        List<Throwable> throwables = putTemplate(request);
        assertEquals(throwables.size(), 1);
        assertThat(throwables.get(0), instanceOf(InvalidIndexTemplateException.class));
        assertThat(throwables.get(0).getMessage(), containsString("name must not contain a space"));
        assertThat(throwables.get(0).getMessage(), containsString("template must not start with '_'"));
        assertThat(throwables.get(0).getMessage(), containsString("index must have 1 or more primary shards"));
    }

    public void testIndexTemplateWithAliasNameEqualToTemplatePattern() {
        PutRequest request = new PutRequest("api", "foobar_template");
        request.template("foobar");
        request.aliases(Collections.singleton(new Alias("foobar")));

        List<Throwable> errors = putTemplate(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(IllegalArgumentException.class));
        assertThat(errors.get(0).getMessage(), equalTo("Alias [foobar] cannot be the same as the template pattern [foobar]"));
    }

    public void testIndexTemplateWithValidateEmptyMapping() throws Exception {
        PutRequest request = new PutRequest("api", "validate_template");
        request.template("validate_template");
        request.putMapping("type1", "{}");

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(MapperParsingException.class));
        assertThat(errors.get(0).getMessage(), containsString("malformed mapping no root object found"));
    }

    public void testIndexTemplateWithValidateMapping() throws Exception {
        PutRequest request = new PutRequest("api", "validate_template");
        request.template("te*");
        request.putMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
            .startObject("field2").field("type", "string").field("analyzer", "custom_1").endObject()
            .endObject().endObject().endObject().string());

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(MapperParsingException.class));
        assertThat(errors.get(0).getMessage(), containsString("analyzer [custom_1] not found for field [field2]"));
    }

    public void testBrokenMapping() throws Exception {
        PutRequest request = new PutRequest("api", "broken_mapping");
        request.template("te*");
        request.putMapping("type1", "abcde");

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(MapperParsingException.class));
        assertThat(errors.get(0).getMessage(), containsString("Failed to parse mapping "));
    }

    public void testBlankMapping() throws Exception {
        PutRequest request = new PutRequest("api", "blank_mapping");
        request.template("te*");
        request.putMapping("type1", "{}");

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(MapperParsingException.class));
        assertThat(errors.get(0).getMessage(), containsString("malformed mapping no root object found"));
    }

    public void testAliasInvalidFilterInvalidJson() throws Exception {
        //invalid json: put index template fails
        PutRequest request = new PutRequest("api", "blank_mapping");
        request.template("te*");
        request.putMapping("type1", "{}");
        Set<Alias> aliases = new HashSet<>();
        aliases.add(new Alias("invalid_alias").filter("abcde"));
        request.aliases(aliases);

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(IllegalArgumentException.class));
        assertThat(errors.get(0).getMessage(), equalTo("failed to parse filter for alias [invalid_alias]"));
    }


    private static List<Throwable> putTemplate(PutRequest request) {
        MetaDataCreateIndexService createIndexService = new MetaDataCreateIndexService(
                Settings.EMPTY,
                null,
                null,
                null,
                Version.CURRENT,
                null,
                new HashSet<>(),
                null,
                null, null);
        MetaDataIndexTemplateService service = new MetaDataIndexTemplateService(Settings.EMPTY, null, createIndexService, new AliasValidator(Settings.EMPTY), null, null);

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

    private List<Throwable> putTemplateDetail(PutRequest request) throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        NodeServicesProvider nodeServicesProvider = getInstanceFromNode(NodeServicesProvider.class);
        MetaDataCreateIndexService createIndexService = new MetaDataCreateIndexService(
            Settings.EMPTY,
            clusterService,
            indicesService,
            null,
            Version.CURRENT,
            null,
            new HashSet<>(),
            null,
            nodeServicesProvider,
            null);
        MetaDataIndexTemplateService service = new MetaDataIndexTemplateService(
            Settings.EMPTY, clusterService, createIndexService, new AliasValidator(Settings.EMPTY), indicesService, nodeServicesProvider);

        final List<Throwable> throwables = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        service.putTemplate(request, new MetaDataIndexTemplateService.PutListener() {
            @Override
            public void onResponse(MetaDataIndexTemplateService.PutResponse response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                throwables.add(t);
                latch.countDown();
            }
        });
        latch.await();
        return throwables;
    }
}
