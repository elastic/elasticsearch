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

import com.fasterxml.jackson.core.JsonParseException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.PutRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

public class MetadataIndexTemplateServiceTests extends ESSingleNodeTestCase {
    public void testIndexTemplateInvalidNumberOfShards() {
        PutRequest request = new PutRequest("test", "test_shards");
        request.patterns(singletonList("test_shards*"));

        request.settings(builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "0")
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

    public void testIndexTemplateValidationWithSpecifiedReplicas() throws Exception {
        PutRequest request = new PutRequest("test", "test_replicas");
        request.patterns(singletonList("test_shards_wait*"));

        Settings.Builder settingsBuilder = builder()
        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
        .put(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), "2");

        request.settings(settingsBuilder.build());

        List<Throwable> throwables = putTemplateDetail(request);

        assertThat(throwables, is(empty()));
    }

    public void testIndexTemplateValidationErrorsWithSpecifiedReplicas() throws Exception {
        PutRequest request = new PutRequest("test", "test_specified_replicas");
        request.patterns(singletonList("test_shards_wait*"));

        Settings.Builder settingsBuilder = builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
            .put(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), "3");

        request.settings(settingsBuilder.build());

        List<Throwable> throwables = putTemplateDetail(request);

        assertThat(throwables.get(0), instanceOf(IllegalArgumentException.class));
        assertThat(throwables.get(0).getMessage(), containsString("[3]: cannot be greater than number of shard copies [2]"));
    }

    public void testIndexTemplateValidationWithDefaultReplicas() throws Exception {
        PutRequest request = new PutRequest("test", "test_default_replicas");
        request.patterns(singletonList("test_wait_shards_default_replica*"));

        Settings.Builder settingsBuilder = builder()
            .put(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), "2");

        request.settings(settingsBuilder.build());

        List<Throwable> throwables = putTemplateDetail(request);

        assertThat(throwables.get(0), instanceOf(IllegalArgumentException.class));
        assertThat(throwables.get(0).getMessage(), containsString("[2]: cannot be greater than number of shard copies [1]"));
    }
    public void testIndexTemplateValidationAccumulatesValidationErrors() {
        PutRequest request = new PutRequest("test", "putTemplate shards");
        request.patterns(singletonList("_test_shards*"));
        request.settings(builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "0").build());

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
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "foo-1234", randomBoolean()).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("foo-2", "foo-1"));
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "bar-xyz", randomBoolean()).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("bar"));
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "baz", randomBoolean()), empty());
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
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "foo-1234", true).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), containsInAnyOrder("foo-2", "foo-1"));
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "bar-xyz", true).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("bar"));
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "baz", true), empty());
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "sneaky1", true).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("sneaky-hidden"));

        // not hidden
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "foo-1234", false).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), containsInAnyOrder("foo-2", "foo-1", "global"));
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "bar-xyz", false).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), containsInAnyOrder("bar", "global"));
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "baz", false).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("global"));
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "sneaky1", false).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), containsInAnyOrder("global", "sneaky-hidden"));

        // unknown
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "foo-1234", null).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), containsInAnyOrder("foo-2", "foo-1", "global"));
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "bar-xyz", null).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), containsInAnyOrder("bar", "global"));
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "baz", null).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("global"));
        assertThat(MetadataIndexTemplateService.findTemplates(state.metadata(), "sneaky1", null).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("sneaky-hidden"));
    }

    public void testPutGlobalTemplateWithIndexHiddenSetting() throws Exception {
        List<Throwable> errors = putTemplateDetail(new PutRequest("testPutGlobalTemplateWithIndexHiddenSetting", "sneaky-hidden")
            .patterns(singletonList("*")).settings(Settings.builder().put("index.hidden", true).build()));
        assertThat(errors.size(), is(1));
        assertThat(errors.get(0).getMessage(), containsString("global templates may not specify the setting index.hidden"));
    }

    public void testAddComponentTemplate() throws Exception{
        MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();
        ClusterState state = ClusterState.EMPTY_STATE;
        Template template = new Template(Settings.builder().build(), null, ComponentTemplateTests.randomAliases());
        ComponentTemplate componentTemplate = new ComponentTemplate(template, 1L, new HashMap<>());
        state = metadataIndexTemplateService.addComponentTemplate(state, false, "foo", componentTemplate);

        assertNotNull(state.metadata().componentTemplates().get("foo"));
        assertThat(state.metadata().componentTemplates().get("foo"), equalTo(componentTemplate));

        final ClusterState throwState = ClusterState.builder(state).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> metadataIndexTemplateService.addComponentTemplate(throwState, true, "foo", componentTemplate));
        assertThat(e.getMessage(), containsString("component template [foo] already exists"));

        state = metadataIndexTemplateService.addComponentTemplate(state, randomBoolean(), "bar", componentTemplate);
        assertNotNull(state.metadata().componentTemplates().get("bar"));

        template = new Template(Settings.builder().build(), new CompressedXContent("{\"invalid\"}"),
            ComponentTemplateTests.randomAliases());
        ComponentTemplate componentTemplate2 = new ComponentTemplate(template, 1L, new HashMap<>());
        expectThrows(JsonParseException.class,
            () -> metadataIndexTemplateService.addComponentTemplate(throwState, true, "foo2", componentTemplate2));

        template = new Template(Settings.builder().build(), new CompressedXContent("{\"invalid\":\"invalid\"}"),
            ComponentTemplateTests.randomAliases());
        ComponentTemplate componentTemplate3 = new ComponentTemplate(template, 1L, new HashMap<>());
        expectThrows(MapperParsingException.class,
            () -> metadataIndexTemplateService.addComponentTemplate(throwState, true, "foo2", componentTemplate3));

        template = new Template(Settings.builder().put("invalid", "invalid").build(), new CompressedXContent("{}"),
            ComponentTemplateTests.randomAliases());
        ComponentTemplate componentTemplate4 = new ComponentTemplate(template, 1L, new HashMap<>());
        expectThrows(IllegalArgumentException.class,
            () -> metadataIndexTemplateService.addComponentTemplate(throwState, true, "foo2", componentTemplate4));
    }

    public void testAddIndexTemplateV2() {
        ClusterState state = ClusterState.EMPTY_STATE;
        IndexTemplateV2 template = IndexTemplateV2Tests.randomInstance();
        state = MetadataIndexTemplateService.addIndexTemplateV2(state, false, "foo", template);

        assertNotNull(state.metadata().templatesV2().get("foo"));
        assertThat(state.metadata().templatesV2().get("foo"), equalTo(template));

        final ClusterState throwState = ClusterState.builder(state).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> MetadataIndexTemplateService.addIndexTemplateV2(throwState, true, "foo", template));
        assertThat(e.getMessage(), containsString("index template [foo] already exists"));

        state = MetadataIndexTemplateService.addIndexTemplateV2(state, randomBoolean(), "bar", template);
        assertNotNull(state.metadata().templatesV2().get("bar"));
    }

    public void testRemoveIndexTemplateV2() {
        IndexTemplateV2 template = IndexTemplateV2Tests.randomInstance();
        IndexTemplateMissingException e = expectThrows(IndexTemplateMissingException.class,
            () -> MetadataIndexTemplateService.innerRemoveIndexTemplateV2(ClusterState.EMPTY_STATE, "foo"));
        assertThat(e.getMessage(), equalTo("index_template [foo] missing"));

        final ClusterState state = MetadataIndexTemplateService.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "foo", template);
        assertNotNull(state.metadata().templatesV2().get("foo"));
        assertThat(state.metadata().templatesV2().get("foo"), equalTo(template));

        ClusterState updatedState = MetadataIndexTemplateService.innerRemoveIndexTemplateV2(state, "foo");
        assertNull(updatedState.metadata().templatesV2().get("foo"));
    }

    /**
     * Test that if we have a pre-existing v1 template and put a v2 template that would match the same indices, we generate a warning
     */
    public void testPuttingV2TemplateGeneratesWarning() {
        IndexTemplateMetadata v1Template = IndexTemplateMetadata.builder("v1-template")
            .patterns(Arrays.asList("fo*", "baz"))
            .build();

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder(Metadata.EMPTY_METADATA)
                .put(v1Template)
                .build())
            .build();

        IndexTemplateV2 v2Template = new IndexTemplateV2(Arrays.asList("foo-bar-*", "eggplant"), null, null, null, null, null);
        state = MetadataIndexTemplateService.addIndexTemplateV2(state, false, "v2-template", v2Template);

        assertWarnings("index template [v2-template] has index patterns [foo-bar-*, eggplant] matching patterns " +
            "from existing older templates [v1-template] with patterns (v1-template => [fo*, baz]); this template [v2-template] will " +
            "take precedence during new index creation");

        assertNotNull(state.metadata().templatesV2().get("v2-template"));
        assertThat(state.metadata().templatesV2().get("v2-template"), equalTo(v2Template));
    }

    /**
     * Test that if we have a pre-existing v2 template and put a "*" v1 template, we generate a warning
     */
    public void testPuttingV1StarTemplateGeneratesWarning() {
        IndexTemplateV2 v2Template = new IndexTemplateV2(Arrays.asList("foo-bar-*", "eggplant"), null, null, null, null, null);
        ClusterState state = MetadataIndexTemplateService.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "v2-template", v2Template);

        MetadataIndexTemplateService.PutRequest req = new MetadataIndexTemplateService.PutRequest("cause", "v1-template");
        req.patterns(Arrays.asList("*", "baz"));
        state = MetadataIndexTemplateService.innerPutTemplate(state, req, IndexTemplateMetadata.builder("v1-template"));

        assertWarnings("template [v1-template] has index patterns [*, baz] matching patterns from existing " +
            "index templates [v2-template] with patterns (v2-template => [foo-bar-*, eggplant]); this template [v1-template] may " +
            "be ignored in favor of an index template at index creation time");

        assertNotNull(state.metadata().templates().get("v1-template"));
        assertThat(state.metadata().templates().get("v1-template").patterns(), containsInAnyOrder("*", "baz"));
    }

    /**
     * Test that if we have a pre-existing v2 template and put a v1 template that would match the same indices, we generate a hard error
     */
    public void testPuttingV1NonStarTemplateGeneratesError() {
        IndexTemplateV2 v2Template = new IndexTemplateV2(Arrays.asList("foo-bar-*", "eggplant"), null, null, null, null, null);
        ClusterState state = MetadataIndexTemplateService.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "v2-template", v2Template);

        MetadataIndexTemplateService.PutRequest req = new MetadataIndexTemplateService.PutRequest("cause", "v1-template");
        req.patterns(Arrays.asList("egg*", "baz"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> MetadataIndexTemplateService.innerPutTemplate(state, req, IndexTemplateMetadata.builder("v1-template")));

        assertThat(e.getMessage(),
            equalTo("template [v1-template] has index patterns [egg*, baz] matching patterns from existing index " +
                "templates [v2-template] with patterns (v2-template => [foo-bar-*, eggplant]), use index templates " +
                "(/_index_template) instead"));

        assertNull(state.metadata().templates().get("v1-template"));
    }

    /**
     * Test that if we have a pre-existing v1 and v2 template, and we update the existing v1
     * template without changing its index patterns, a warning is generated
     */
    public void testUpdatingV1NonStarTemplateWithUnchangedPatternsGeneratesWarning() {
        IndexTemplateMetadata v1Template = IndexTemplateMetadata.builder("v1-template")
            .patterns(Arrays.asList("fo*", "baz"))
            .build();

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder(Metadata.EMPTY_METADATA)
                .put(v1Template)
                .build())
            .build();

        IndexTemplateV2 v2Template = new IndexTemplateV2(Arrays.asList("foo-bar-*", "eggplant"), null, null, null, null, null);
        state = MetadataIndexTemplateService.addIndexTemplateV2(state, false, "v2-template", v2Template);

        assertWarnings("index template [v2-template] has index patterns [foo-bar-*, eggplant] matching patterns " +
            "from existing older templates [v1-template] with patterns (v1-template => [fo*, baz]); this template [v2-template] will " +
            "take precedence during new index creation");

        assertNotNull(state.metadata().templatesV2().get("v2-template"));
        assertThat(state.metadata().templatesV2().get("v2-template"), equalTo(v2Template));

        // Now try to update the existing v1-template

        MetadataIndexTemplateService.PutRequest req = new MetadataIndexTemplateService.PutRequest("cause", "v1-template");
        req.patterns(Arrays.asList("fo*", "baz"));
        state = MetadataIndexTemplateService.innerPutTemplate(state, req, IndexTemplateMetadata.builder("v1-template"));

        assertWarnings("template [v1-template] has index patterns [fo*, baz] matching patterns from existing " +
            "index templates [v2-template] with patterns (v2-template => [foo-bar-*, eggplant]); this template [v1-template] may " +
            "be ignored in favor of an index template at index creation time");

        assertNotNull(state.metadata().templates().get("v1-template"));
        assertThat(state.metadata().templates().get("v1-template").patterns(), containsInAnyOrder("fo*", "baz"));
    }

    /**
     * Test that if we have a pre-existing v1 and v2 template, and we update the existing v1
     * template *AND* change the index patterns that an error is generated
     */
    public void testUpdatingV1NonStarWithChangedPatternsTemplateGeneratesError() {
        IndexTemplateMetadata v1Template = IndexTemplateMetadata.builder("v1-template")
            .patterns(Arrays.asList("fo*", "baz"))
            .build();

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder(Metadata.EMPTY_METADATA)
                .put(v1Template)
                .build())
            .build();

        IndexTemplateV2 v2Template = new IndexTemplateV2(Arrays.asList("foo-bar-*", "eggplant"), null, null, null, null, null);
        state = MetadataIndexTemplateService.addIndexTemplateV2(state, false, "v2-template", v2Template);

        assertWarnings("index template [v2-template] has index patterns [foo-bar-*, eggplant] matching patterns " +
            "from existing older templates [v1-template] with patterns (v1-template => [fo*, baz]); this template [v2-template] will " +
            "take precedence during new index creation");

        assertNotNull(state.metadata().templatesV2().get("v2-template"));
        assertThat(state.metadata().templatesV2().get("v2-template"), equalTo(v2Template));

        // Now try to update the existing v1-template

        MetadataIndexTemplateService.PutRequest req = new MetadataIndexTemplateService.PutRequest("cause", "v1-template");
        req.patterns(Arrays.asList("egg*", "baz"));
        final ClusterState finalState = state;
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> MetadataIndexTemplateService.innerPutTemplate(finalState, req, IndexTemplateMetadata.builder("v1-template")));

        assertThat(e.getMessage(), equalTo("template [v1-template] has index patterns [egg*, baz] matching patterns " +
            "from existing index templates [v2-template] with patterns (v2-template => [foo-bar-*, eggplant]), use index " +
            "templates (/_index_template) instead"));
    }

    private static List<Throwable> putTemplate(NamedXContentRegistry xContentRegistry, PutRequest request) {
        MetadataCreateIndexService createIndexService = new MetadataCreateIndexService(
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
        MetadataIndexTemplateService service = new MetadataIndexTemplateService(null, createIndexService,
                new AliasValidator(), null,
                new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS), xContentRegistry);

        final List<Throwable> throwables = new ArrayList<>();
        service.putTemplate(request, new MetadataIndexTemplateService.PutListener() {
            @Override
            public void onResponse(MetadataIndexTemplateService.PutResponse response) {

            }

            @Override
            public void onFailure(Exception e) {
                throwables.add(e);
            }
        });
        return throwables;
    }

    private List<Throwable> putTemplateDetail(PutRequest request) throws Exception {
        MetadataIndexTemplateService service = getMetadataIndexTemplateService();

        final List<Throwable> throwables = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        service.putTemplate(request, new MetadataIndexTemplateService.PutListener() {
            @Override
            public void onResponse(MetadataIndexTemplateService.PutResponse response) {
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

    private MetadataIndexTemplateService getMetadataIndexTemplateService() {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        MetadataCreateIndexService createIndexService = new MetadataCreateIndexService(
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
        return new MetadataIndexTemplateService(
                clusterService, createIndexService, new AliasValidator(), indicesService,
                new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS), xContentRegistry());
    }
}
