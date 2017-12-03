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

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService.PutRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
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
        request.patterns(Collections.singletonList("test_shards*"));

        request.settings(Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "0")
            .put("index.shard.check_on_startup", "blargh").build());

        List<Throwable> throwables = putTemplate(xContentRegistry(), request);
        assertEquals(throwables.size(), 1);
        assertThat(throwables.get(0), instanceOf(InvalidIndexTemplateException.class));
        assertThat(throwables.get(0).getMessage(),
                containsString("Failed to parse value [0] for setting [index.number_of_shards] must be >= 1"));
        assertThat(throwables.get(0).getMessage(),
                containsString("unknown value for [index.shard.check_on_startup] " +
                                "must be one of [true, false, fix, checksum] but was: blargh"));
    }

    public void testIndexTemplateValidationAccumulatesValidationErrors() {
        PutRequest request = new PutRequest("test", "putTemplate shards");
        request.patterns(Collections.singletonList("_test_shards*"));
        request.settings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "0").build());

        List<Throwable> throwables = putTemplate(xContentRegistry(), request);
        assertEquals(throwables.size(), 1);
        assertThat(throwables.get(0), instanceOf(InvalidIndexTemplateException.class));
        assertThat(throwables.get(0).getMessage(), containsString("name must not contain a space"));
        assertThat(throwables.get(0).getMessage(), containsString("template must not start with '_'"));
        assertThat(throwables.get(0).getMessage(),
                containsString("Failed to parse value [0] for setting [index.number_of_shards] must be >= 1"));
    }

    public void testIndexTemplateWithAliasNameEqualToTemplatePattern() {
        PutRequest request = new PutRequest("api", "foobar_template");
        request.patterns(Arrays.asList("foo", "foobar"));
        request.aliases(Collections.singleton(new Alias("foobar")));

        List<Throwable> errors = putTemplate(xContentRegistry(), request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(IllegalArgumentException.class));
        assertThat(errors.get(0).getMessage(), equalTo("Alias [foobar] cannot be the same as any pattern in [foo, foobar]"));
    }

    public void testIndexTemplateWithValidateEmptyMapping() throws Exception {
        PutRequest request = new PutRequest("api", "validate_template");
        request.patterns(Collections.singletonList("validate_template"));
        request.putMapping("type1", "{}");

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(MapperParsingException.class));
        assertThat(errors.get(0).getMessage(), containsString("malformed mapping no root object found"));
    }

    public void testIndexTemplateWithValidateMapping() throws Exception {
        PutRequest request = new PutRequest("api", "validate_template");
        request.patterns(Collections.singletonList("te*"));
        request.putMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
            .startObject("field2").field("type", "text").field("analyzer", "custom_1").endObject()
            .endObject().endObject().endObject().string());

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(MapperParsingException.class));
        assertThat(errors.get(0).getMessage(), containsString("analyzer [custom_1] not found for field [field2]"));
    }

    public void testBrokenMapping() throws Exception {
        PutRequest request = new PutRequest("api", "broken_mapping");
        request.patterns(Collections.singletonList("te*"));
        request.putMapping("type1", "abcde");

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(MapperParsingException.class));
        assertThat(errors.get(0).getMessage(), containsString("Failed to parse mapping "));
    }

    public void testBlankMapping() throws Exception {
        PutRequest request = new PutRequest("api", "blank_mapping");
        request.patterns(Collections.singletonList("te*"));
        request.putMapping("type1", "{}");

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(MapperParsingException.class));
        assertThat(errors.get(0).getMessage(), containsString("malformed mapping no root object found"));
    }

    public void testAliasInvalidFilterInvalidJson() throws Exception {
        //invalid json: put index template fails
        PutRequest request = new PutRequest("api", "blank_mapping");
        request.patterns(Collections.singletonList("te*"));
        request.putMapping("type1", "{}");
        Set<Alias> aliases = new HashSet<>();
        aliases.add(new Alias("invalid_alias").filter("abcde"));
        request.aliases(aliases);

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(IllegalArgumentException.class));
        assertThat(errors.get(0).getMessage(), equalTo("failed to parse filter for alias [invalid_alias]"));
    }


    private static List<Throwable> putTemplate(NamedXContentRegistry xContentRegistry, PutRequest request) {
        MetaDataCreateIndexService createIndexService = new MetaDataCreateIndexService(
                Settings.EMPTY,
                null,
                null,
                null,
                null,
                null, null, null, xContentRegistry);
        MetaDataIndexTemplateService service = new MetaDataIndexTemplateService(Settings.EMPTY, null, createIndexService,
                new AliasValidator(Settings.EMPTY), null,
                new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS), xContentRegistry);

        final List<Throwable> throwables = new ArrayList<>();
        service.putTemplate(request, new MetaDataIndexTemplateService.PutListener() {
            @Override
            public void onResponse(MetaDataIndexTemplateService.PutResponse response) {

            }

            @Override
            public void onFailure(Exception e) {
                throwables.add(e);
            }
        });
        return throwables;
    }

    private List<Throwable> putTemplateDetail(PutRequest request) throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        MetaDataCreateIndexService createIndexService = new MetaDataCreateIndexService(
            Settings.EMPTY,
            clusterService,
            indicesService,
            null,
            null,
            null,
            null,
            null,
            xContentRegistry());
        MetaDataIndexTemplateService service = new MetaDataIndexTemplateService(
                Settings.EMPTY, clusterService, createIndexService, new AliasValidator(Settings.EMPTY), indicesService,
                new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS), xContentRegistry());

        final List<Throwable> throwables = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        service.putTemplate(request, new MetaDataIndexTemplateService.PutListener() {
            @Override
            public void onResponse(MetaDataIndexTemplateService.PutResponse response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throwables.add(e);
                latch.countDown();
            }
        });
        latch.await();
        return throwables;
    }
}
