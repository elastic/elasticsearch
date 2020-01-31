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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService.PutRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.settings.Settings.builder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class MetaDataIndexTemplateServiceTests extends ESSingleNodeTestCase {
    public void testIndexTemplateInvalidNumberOfShards() {
        PutRequest request = new PutRequest("test", "test_shards");
        request.patterns(singletonList("test_shards*"));

        request.settings(builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "0")
            .put("index.shard.check_on_startup", "blargh").build());

        List<Throwable> throwables = putTemplate(xContentRegistry(), request);
        assertEquals(throwables.size(), 1);
        assertThat(throwables.get(0), instanceOf(InvalidIndexTemplateException.class));
        assertThat(throwables.get(0).getMessage(),
                containsString("Failed to parse value [0] for setting [index.number_of_shards] must be >= 1"));
        assertThat(throwables.get(0).getMessage(),
                containsString("unknown value for [index.shard.check_on_startup] " +
                                "must be one of [true, false, checksum] but was: blargh"));
    }

    public void testIndexTemplateValidationAccumulatesValidationErrors() {
        PutRequest request = new PutRequest("test", "putTemplate shards");
        request.patterns(singletonList("_test_shards*"));
        request.settings(builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "0").build());

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

    public void testIndexTemplateWithValidateMapping() throws Exception {
        PutRequest request = new PutRequest("api", "validate_template");
        request.patterns(singletonList("te*"));
        request.mappings(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                        .startObject("properties").startObject("field2").field("type", "text").field("analyzer", "custom_1").endObject()
                        .endObject().endObject().endObject()));

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(MapperParsingException.class));
        assertThat(errors.get(0).getMessage(), containsString("analyzer [custom_1] not found for field [field2]"));
    }

    public void testBrokenMapping() throws Exception {
        PutRequest request = new PutRequest("api", "broken_mapping");
        request.patterns(singletonList("te*"));
        request.mappings("abcde");

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(MapperParsingException.class));
        assertThat(errors.get(0).getMessage(), containsString("Failed to parse mapping"));
    }

    public void testAliasInvalidFilterInvalidJson() throws Exception {
        //invalid json: put index template fails
        PutRequest request = new PutRequest("api", "blank_mapping");
        request.patterns(singletonList("te*"));
        request.mappings("{}");
        Set<Alias> aliases = new HashSet<>();
        aliases.add(new Alias("invalid_alias").filter("abcde"));
        request.aliases(aliases);

        List<Throwable> errors = putTemplateDetail(request);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0), instanceOf(IllegalArgumentException.class));
        assertThat(errors.get(0).getMessage(), equalTo("failed to parse filter for alias [invalid_alias]"));
    }

    public void testFindTemplates() throws Exception {
        client().admin().indices().prepareDeleteTemplate("*").get(); // Delete all existing templates
        putTemplateDetail(new PutRequest("test", "foo-1").patterns(singletonList("foo-*")).order(1));
        putTemplateDetail(new PutRequest("test", "foo-2").patterns(singletonList("foo-*")).order(2));
        putTemplateDetail(new PutRequest("test", "bar").patterns(singletonList("bar-*")).order(between(0, 100)));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "foo-1234", randomBoolean()).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), contains("foo-2", "foo-1"));
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "bar-xyz", randomBoolean()).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), contains("bar"));
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "baz", randomBoolean()), empty());
    }

    public void testFindTemplatesWithHiddenIndices() throws Exception {
        client().admin().indices().prepareDeleteTemplate("*").get(); // Delete all existing templates
        putTemplateDetail(new PutRequest("testFindTemplatesWithHiddenIndices", "foo-1").patterns(singletonList("foo-*")).order(1));
        putTemplateDetail(new PutRequest("testFindTemplatesWithHiddenIndices", "foo-2").patterns(singletonList("foo-*")).order(2));
        putTemplateDetail(
            new PutRequest("testFindTemplatesWithHiddenIndices", "bar").patterns(singletonList("bar-*")).order(between(0, 100)));
        putTemplateDetail(new PutRequest("testFindTemplatesWithHiddenIndices", "global").patterns(singletonList("*")));
        putTemplateDetail(new PutRequest("testFindTemplatesWithHiddenIndices", "sneaky-hidden")
            .patterns(singletonList("sneaky*")).settings(Settings.builder().put("index.hidden", true).build()));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();

        // hidden
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "foo-1234", true).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), containsInAnyOrder("foo-2", "foo-1"));
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "bar-xyz", true).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), contains("bar"));
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "baz", true), empty());
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "sneaky1", true).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), contains("sneaky-hidden"));

        // not hidden
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "foo-1234", false).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), containsInAnyOrder("foo-2", "foo-1", "global"));
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "bar-xyz", false).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), containsInAnyOrder("bar", "global"));
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "baz", false).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), contains("global"));
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "sneaky1", false).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), containsInAnyOrder("global", "sneaky-hidden"));

        // unknown
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "foo-1234", null).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), containsInAnyOrder("foo-2", "foo-1", "global"));
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "bar-xyz", null).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), containsInAnyOrder("bar", "global"));
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "baz", null).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), contains("global"));
        assertThat(MetaDataIndexTemplateService.findTemplates(state.metaData(), "sneaky1", null).stream()
            .map(IndexTemplateMetaData::name).collect(Collectors.toList()), contains("sneaky-hidden"));
    }

    public void testPutGlobalTemplateWithIndexHiddenSetting() throws Exception {
        List<Throwable> errors = putTemplateDetail(new PutRequest("testPutGlobalTemplateWithIndexHiddenSetting", "sneaky-hidden")
            .patterns(singletonList("*")).settings(Settings.builder().put("index.hidden", true).build()));
        assertThat(errors.size(), is(1));
        assertThat(errors.get(0).getMessage(), containsString("global templates may not specify the setting index.hidden"));
    }

    private static List<Throwable> putTemplate(NamedXContentRegistry xContentRegistry, PutRequest request) {
        MetaDataCreateIndexService createIndexService = new MetaDataCreateIndexService(
                Settings.EMPTY,
                null,
                null,
                null,
                null,
                new Environment(builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build(), null),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                null,
                xContentRegistry,
                Collections.emptyList(),
                true);
        MetaDataIndexTemplateService service = new MetaDataIndexTemplateService(null, createIndexService,
                new AliasValidator(), null,
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
                new Environment(builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build(), null),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                null,
                xContentRegistry(),
                Collections.emptyList(),
                true);
        MetaDataIndexTemplateService service = new MetaDataIndexTemplateService(
                clusterService, createIndexService, new AliasValidator(), indicesService,
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
