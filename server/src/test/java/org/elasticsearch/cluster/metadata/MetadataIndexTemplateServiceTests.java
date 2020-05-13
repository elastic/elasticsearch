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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.PutRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.settings.Settings.builder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
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
        assertThat(throwables.get(0).getMessage(), containsString("index_pattern [_test_shards*] must not start with '_'"));
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
        assertThat(errors.get(0).getMessage(), equalTo("alias [foobar] cannot be the same as any pattern in [foo, foobar]"));
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
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "foo-1234", randomBoolean()).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("foo-2", "foo-1"));
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "bar-xyz", randomBoolean()).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("bar"));
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "baz", randomBoolean()), empty());
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
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "foo-1234", true).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), containsInAnyOrder("foo-2", "foo-1"));
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "bar-xyz", true).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("bar"));
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "baz", true), empty());
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "sneaky1", true).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("sneaky-hidden"));

        // not hidden
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "foo-1234", false).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), containsInAnyOrder("foo-2", "foo-1", "global"));
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "bar-xyz", false).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), containsInAnyOrder("bar", "global"));
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "baz", false).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("global"));
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "sneaky1", false).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), containsInAnyOrder("global", "sneaky-hidden"));

        // unknown
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "foo-1234", null).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), containsInAnyOrder("foo-2", "foo-1", "global"));
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "bar-xyz", null).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), containsInAnyOrder("bar", "global"));
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "baz", null).stream()
            .map(IndexTemplateMetadata::name).collect(Collectors.toList()), contains("global"));
        assertThat(MetadataIndexTemplateService.findV1Templates(state.metadata(), "sneaky1", null).stream()
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

    public void testUpdateComponentTemplateWithIndexHiddenSetting() throws Exception {
        MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();
        ClusterState state = ClusterState.EMPTY_STATE;
        Template template = new Template(Settings.builder().build(), null, ComponentTemplateTests.randomAliases());
        ComponentTemplate componentTemplate = new ComponentTemplate(template, 1L, new HashMap<>());
        state = metadataIndexTemplateService.addComponentTemplate(state, true, "foo", componentTemplate);
        assertNotNull(state.metadata().componentTemplates().get("foo"));

        IndexTemplateV2 firstGlobalIndexTemplate =
            new IndexTemplateV2(List.of("*"), template, List.of("foo"), 1L, null, null, null);
        state = metadataIndexTemplateService.addIndexTemplateV2(state, true, "globalindextemplate1", firstGlobalIndexTemplate);

        IndexTemplateV2 secondGlobalIndexTemplate =
            new IndexTemplateV2(List.of("*"), template, List.of("foo"), 2L, null, null, null);
        state = metadataIndexTemplateService.addIndexTemplateV2(state, true, "globalindextemplate2", secondGlobalIndexTemplate);

        IndexTemplateV2 fooPatternIndexTemplate =
            new IndexTemplateV2(List.of("foo-*"), template, List.of("foo"), 3L, null, null, null);
        state = metadataIndexTemplateService.addIndexTemplateV2(state, true, "foopatternindextemplate", fooPatternIndexTemplate);

        // update the component template to set the index.hidden setting
        Template templateWithIndexHiddenSetting = new Template(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build(),
            null, null);
        ComponentTemplate updatedComponentTemplate = new ComponentTemplate(templateWithIndexHiddenSetting, 2L, new HashMap<>());
        try {
            metadataIndexTemplateService.addComponentTemplate(state, false, "foo", updatedComponentTemplate);
            fail("expecting an exception as updating the component template would yield the global templates to include the index.hidden " +
                "setting");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsStringIgnoringCase("globalindextemplate1"));
            assertThat(e.getMessage(), containsStringIgnoringCase("globalindextemplate2"));
            assertThat(e.getMessage(), not(containsStringIgnoringCase("foopatternindextemplate")));
        }
    }

    public void testAddIndexTemplateV2() throws Exception {
        ClusterState state = ClusterState.EMPTY_STATE;
        final MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();
        IndexTemplateV2 template = IndexTemplateV2Tests.randomInstance();
        state = metadataIndexTemplateService.addIndexTemplateV2(state, false, "foo", template);

        assertNotNull(state.metadata().templatesV2().get("foo"));
        assertTemplatesEqual(state.metadata().templatesV2().get("foo"), template);


        IndexTemplateV2 newTemplate = randomValueOtherThanMany(t -> Objects.equals(template.priority(), t.priority()),
            IndexTemplateV2Tests::randomInstance);

        final ClusterState throwState = ClusterState.builder(state).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> metadataIndexTemplateService.addIndexTemplateV2(throwState, true, "foo", newTemplate));
        assertThat(e.getMessage(), containsString("index template [foo] already exists"));

        state = metadataIndexTemplateService.addIndexTemplateV2(state, randomBoolean(), "bar", newTemplate);
        assertNotNull(state.metadata().templatesV2().get("bar"));
    }

    public void testUpdateIndexTemplateV2() throws Exception {
        ClusterState state = ClusterState.EMPTY_STATE;
        final MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();
        IndexTemplateV2 template = IndexTemplateV2Tests.randomInstance();
        state = metadataIndexTemplateService.addIndexTemplateV2(state, false, "foo", template);

        assertNotNull(state.metadata().templatesV2().get("foo"));
        assertTemplatesEqual(state.metadata().templatesV2().get("foo"), template);

        List<String> patterns = new ArrayList<>(template.indexPatterns());
        patterns.add("new-pattern");
        template = new IndexTemplateV2(patterns, template.template(), template.composedOf(), template.priority(), template.version(),
            template.metadata(), null);
        state = metadataIndexTemplateService.addIndexTemplateV2(state, false, "foo", template);

        assertNotNull(state.metadata().templatesV2().get("foo"));
        assertTemplatesEqual(state.metadata().templatesV2().get("foo"), template);
    }

    public void testRemoveIndexTemplateV2() throws Exception {
        IndexTemplateV2 template = IndexTemplateV2Tests.randomInstance();
        final MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();
        IndexTemplateMissingException e = expectThrows(IndexTemplateMissingException.class,
            () -> MetadataIndexTemplateService.innerRemoveIndexTemplateV2(ClusterState.EMPTY_STATE, "foo"));
        assertThat(e.getMessage(), equalTo("index_template [foo] missing"));

        final ClusterState state = metadataIndexTemplateService.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "foo", template);
        assertNotNull(state.metadata().templatesV2().get("foo"));
        assertTemplatesEqual(state.metadata().templatesV2().get("foo"), template);

        ClusterState updatedState = MetadataIndexTemplateService.innerRemoveIndexTemplateV2(state, "foo");
        assertNull(updatedState.metadata().templatesV2().get("foo"));
    }

    /**
     * Test that if we have a pre-existing v1 template and put a v2 template that would match the same indices, we generate a warning
     */
    public void testPuttingV2TemplateGeneratesWarning() throws Exception {
        IndexTemplateMetadata v1Template = IndexTemplateMetadata.builder("v1-template")
            .patterns(Arrays.asList("fo*", "baz"))
            .build();
        final MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder(Metadata.EMPTY_METADATA)
                .put(v1Template)
                .build())
            .build();

        IndexTemplateV2 v2Template = new IndexTemplateV2(Arrays.asList("foo-bar-*", "eggplant"), null, null, null, null, null, null);
        state = metadataIndexTemplateService.addIndexTemplateV2(state, false, "v2-template", v2Template);

        assertWarnings("index template [v2-template] has index patterns [foo-bar-*, eggplant] matching patterns " +
            "from existing older templates [v1-template] with patterns (v1-template => [fo*, baz]); this template [v2-template] will " +
            "take precedence during new index creation");

        assertNotNull(state.metadata().templatesV2().get("v2-template"));
        assertTemplatesEqual(state.metadata().templatesV2().get("v2-template"), v2Template);
    }

    public void testPutGlobalV2TemplateWhichResolvesIndexHiddenSetting() throws Exception {
        MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();
        Template templateWithIndexHiddenSetting = new Template(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build(),
            null, null);
        ComponentTemplate componentTemplate = new ComponentTemplate(templateWithIndexHiddenSetting, 1L, new HashMap<>());

        CountDownLatch waitToCreateComponentTemplate = new CountDownLatch(1);
        ActionListener<AcknowledgedResponse> createComponentTemplateListener = new ActionListener<>() {

            @Override
            public void onResponse(AcknowledgedResponse response) {
                waitToCreateComponentTemplate.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("expecting the component template PUT to succeed but got: " + e.getMessage());
            }
        };

        metadataIndexTemplateService.putComponentTemplate("test", true, "ct-with-index-hidden-setting", TimeValue.timeValueSeconds(30L),
            componentTemplate, createComponentTemplateListener);

        waitToCreateComponentTemplate.await(10, TimeUnit.SECONDS);

        IndexTemplateV2 globalIndexTemplate = new IndexTemplateV2(List.of("*"), null, List.of("ct-with-index-hidden-setting"), null, null,
            null, null);

        expectThrows(InvalidIndexTemplateException.class, () ->
            metadataIndexTemplateService.putIndexTemplateV2("testing", true, "template-referencing-ct-with-hidden-index-setting",
                TimeValue.timeValueSeconds(30L), globalIndexTemplate, new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        fail("the listener should not be invoked as the validation should be executed before any cluster state updates " +
                            "are issued");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("the listener should not be invoked as the validation should be executed before any cluster state updates " +
                            "are issued");
                    }
                }));
    }

    /**
     * Test that if we have a pre-existing v2 template and put a "*" v1 template, we generate a warning
     */
    public void testPuttingV1StarTemplateGeneratesWarning() throws Exception {
        final MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();
        IndexTemplateV2 v2Template = new IndexTemplateV2(Arrays.asList("foo-bar-*", "eggplant"), null, null, null, null, null, null);
        ClusterState state = metadataIndexTemplateService.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "v2-template", v2Template);

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
    public void testPuttingV1NonStarTemplateGeneratesError() throws Exception {
        final MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();
        IndexTemplateV2 v2Template = new IndexTemplateV2(Arrays.asList("foo-bar-*", "eggplant"), null, null, null, null, null, null);
        ClusterState state = metadataIndexTemplateService.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "v2-template", v2Template);

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
    public void testUpdatingV1NonStarTemplateWithUnchangedPatternsGeneratesWarning() throws Exception {
        final MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();

        IndexTemplateMetadata v1Template = IndexTemplateMetadata.builder("v1-template")
            .patterns(Arrays.asList("fo*", "baz"))
            .build();

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder(Metadata.EMPTY_METADATA)
                .put(v1Template)
                .build())
            .build();

        IndexTemplateV2 v2Template = new IndexTemplateV2(Arrays.asList("foo-bar-*", "eggplant"), null, null, null, null, null, null);
        state = metadataIndexTemplateService.addIndexTemplateV2(state, false, "v2-template", v2Template);

        assertWarnings("index template [v2-template] has index patterns [foo-bar-*, eggplant] matching patterns " +
            "from existing older templates [v1-template] with patterns (v1-template => [fo*, baz]); this template [v2-template] will " +
            "take precedence during new index creation");

        assertNotNull(state.metadata().templatesV2().get("v2-template"));
        assertTemplatesEqual(state.metadata().templatesV2().get("v2-template"), v2Template);

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
    public void testUpdatingV1NonStarWithChangedPatternsTemplateGeneratesError() throws Exception {
        final MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();
        IndexTemplateMetadata v1Template = IndexTemplateMetadata.builder("v1-template")
            .patterns(Arrays.asList("fo*", "baz"))
            .build();

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder(Metadata.EMPTY_METADATA)
                .put(v1Template)
                .build())
            .build();

        IndexTemplateV2 v2Template = new IndexTemplateV2(Arrays.asList("foo-bar-*", "eggplant"), null, null, null, null, null, null);
        state = metadataIndexTemplateService.addIndexTemplateV2(state, false, "v2-template", v2Template);

        assertWarnings("index template [v2-template] has index patterns [foo-bar-*, eggplant] matching patterns " +
            "from existing older templates [v1-template] with patterns (v1-template => [fo*, baz]); this template [v2-template] will " +
            "take precedence during new index creation");

        assertNotNull(state.metadata().templatesV2().get("v2-template"));
        assertTemplatesEqual(state.metadata().templatesV2().get("v2-template"), v2Template);

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

    public void testPuttingOverlappingV2Template() throws Exception {
        {
            IndexTemplateV2 template = new IndexTemplateV2(Arrays.asList("egg*", "baz"), null, null, 1L, null, null, null);
            MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();
            ClusterState state = metadataIndexTemplateService.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "foo", template);
            IndexTemplateV2 newTemplate = new IndexTemplateV2(Arrays.asList("abc", "baz*"), null, null, 1L, null, null, null);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> metadataIndexTemplateService.addIndexTemplateV2(state, false, "foo2", newTemplate));
            assertThat(e.getMessage(), equalTo("index template [foo2] has index patterns [abc, baz*] matching patterns from existing " +
                "templates [foo] with patterns (foo => [egg*, baz]) that have the same priority [1], multiple index templates may not " +
                "match during index creation, please use a different priority"));
        }

        {
            IndexTemplateV2 template = new IndexTemplateV2(Arrays.asList("egg*", "baz"), null, null, null, null, null, null);
            MetadataIndexTemplateService metadataIndexTemplateService = getMetadataIndexTemplateService();
            ClusterState state = metadataIndexTemplateService.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "foo", template);
            IndexTemplateV2 newTemplate = new IndexTemplateV2(Arrays.asList("abc", "baz*"), null, null, 0L, null, null, null);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> metadataIndexTemplateService.addIndexTemplateV2(state, false, "foo2", newTemplate));
            assertThat(e.getMessage(), equalTo("index template [foo2] has index patterns [abc, baz*] matching patterns from existing " +
                "templates [foo] with patterns (foo => [egg*, baz]) that have the same priority [0], multiple index templates may not " +
                "match during index creation, please use a different priority"));
        }
    }

    public void testFindV2Templates() throws Exception {
        final MetadataIndexTemplateService service = getMetadataIndexTemplateService();
        ClusterState state = ClusterState.EMPTY_STATE;
        assertNull(MetadataIndexTemplateService.findV2Template(state.metadata(), "index", randomBoolean()));

        ComponentTemplate ct = ComponentTemplateTests.randomInstance();
        state = service.addComponentTemplate(state, true, "ct", ct);
        IndexTemplateV2 it = new IndexTemplateV2(List.of("i*"), null, List.of("ct"), null, 1L, null, null);
        state = service.addIndexTemplateV2(state, true, "my-template", it);
        IndexTemplateV2 it2 = new IndexTemplateV2(List.of("in*"), null, List.of("ct"), 10L, 2L, null, null);
        state = service.addIndexTemplateV2(state, true, "my-template2", it2);

        String result = MetadataIndexTemplateService.findV2Template(state.metadata(), "index", randomBoolean());

        assertThat(result, equalTo("my-template2"));
    }

    public void testFindV2TemplatesForHiddenIndex() throws Exception {
        final MetadataIndexTemplateService service = getMetadataIndexTemplateService();
        ClusterState state = ClusterState.EMPTY_STATE;
        assertNull(MetadataIndexTemplateService.findV2Template(state.metadata(), "index", true));

        ComponentTemplate ct = ComponentTemplateTests.randomInstance();
        state = service.addComponentTemplate(state, true, "ct", ct);
        IndexTemplateV2 it = new IndexTemplateV2(List.of("i*"), null, List.of("ct"), 0L, 1L, null, null);
        state = service.addIndexTemplateV2(state, true, "my-template", it);
        IndexTemplateV2 it2 = new IndexTemplateV2(List.of("*"), null, List.of("ct"), 10L, 2L, null, null);
        state = service.addIndexTemplateV2(state, true, "my-template2", it2);

        String result = MetadataIndexTemplateService.findV2Template(state.metadata(), "index", true);

        assertThat(result, equalTo("my-template"));
    }

    public void testFindV2InvalidGlobalTemplate() {
        Template templateWithHiddenSetting = new Template(builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build(), null, null);
        try {
            // add an invalid global template that specifies the `index.hidden` setting
            IndexTemplateV2 invalidGlobalTemplate = new IndexTemplateV2(List.of("*"), templateWithHiddenSetting, List.of("ct"), 5L, 1L,
                null, null);
            Metadata invalidGlobalTemplateMetadata = Metadata.builder().putCustom(IndexTemplateV2Metadata.TYPE,
                new IndexTemplateV2Metadata(Map.of("invalid_global_template", invalidGlobalTemplate))).build();

            MetadataIndexTemplateService.findV2Template(invalidGlobalTemplateMetadata, "index-name", false);
            fail("expecting an exception as the matching global template is invalid");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("global index template [invalid_global_template], composed of component templates [ct] " +
                "defined the index.hidden setting, which is not allowed"));
        }
    }

    public void testResolveMappings() throws Exception {
        final MetadataIndexTemplateService service = getMetadataIndexTemplateService();
        ClusterState state = ClusterState.EMPTY_STATE;

        ComponentTemplate ct1 = new ComponentTemplate(new Template(null,
            new CompressedXContent("{\n" +
                "      \"properties\": {\n" +
                "        \"field2\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        }\n" +
                "      }\n" +
                "    }"), null), null, null);
        ComponentTemplate ct2 = new ComponentTemplate(new Template(null,
            new CompressedXContent("{\n" +
                "      \"properties\": {\n" +
                "        \"field2\": {\n" +
                "          \"type\": \"text\"\n" +
                "        }\n" +
                "      }\n" +
                "    }"), null), null, null);
        state = service.addComponentTemplate(state, true, "ct_high", ct1);
        state = service.addComponentTemplate(state, true, "ct_low", ct2);
        IndexTemplateV2 it = new IndexTemplateV2(List.of("i*"),
            new Template(null,
                new CompressedXContent("{\n" +
                    "    \"properties\": {\n" +
                    "      \"field\": {\n" +
                    "        \"type\": \"keyword\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }"), null),
            List.of("ct_low", "ct_high"), 0L, 1L, null, null);
        state = service.addIndexTemplateV2(state, true, "my-template", it);

        List<CompressedXContent> mappings = MetadataIndexTemplateService.resolveMappings(state, "my-template");

        assertNotNull(mappings);
        assertThat(mappings.size(), equalTo(3));
        List<Map<String, Object>> parsedMappings = mappings.stream()
            .map(m -> {
                try {
                    return MapperService.parseMapping(new NamedXContentRegistry(List.of()), m.string());
                } catch (Exception e) {
                    logger.error(e);
                    fail("failed to parse mappings: " + m.string());
                    return null;
                }
            })
            .collect(Collectors.toList());

        // The order of mappings should be:
        // - ct_low
        // - ct_high
        // - index template
        // Because the first elements when merging mappings have the lowest precedence
        assertThat(parsedMappings.get(0),
            equalTo(Map.of("_doc", Map.of("properties", Map.of("field2", Map.of("type", "text"))))));
        assertThat(parsedMappings.get(1),
            equalTo(Map.of("_doc", Map.of("properties", Map.of("field2", Map.of("type", "keyword"))))));
        assertThat(parsedMappings.get(2),
            equalTo(Map.of("_doc", Map.of("properties", Map.of("field", Map.of("type", "keyword"))))));
    }

    public void testResolveSettings() throws Exception {
        final MetadataIndexTemplateService service = getMetadataIndexTemplateService();
        ClusterState state = ClusterState.EMPTY_STATE;

        ComponentTemplate ct1 = new ComponentTemplate(new Template(Settings.builder()
            .put("number_of_replicas", 2)
            .put("index.blocks.write", true)
            .build(),
            null, null), null, null);
        ComponentTemplate ct2 = new ComponentTemplate(new Template(Settings.builder()
            .put("index.number_of_replicas", 1)
            .put("index.blocks.read", true)
            .build(),
            null, null), null, null);
        state = service.addComponentTemplate(state, true, "ct_high", ct1);
        state = service.addComponentTemplate(state, true, "ct_low", ct2);
        IndexTemplateV2 it = new IndexTemplateV2(List.of("i*"),
            new Template(Settings.builder()
                .put("index.blocks.write", false)
                .put("index.number_of_shards", 3)
                .build(), null, null),
            List.of("ct_low", "ct_high"), 0L, 1L, null, null);
        state = service.addIndexTemplateV2(state, true, "my-template", it);

        Settings settings = MetadataIndexTemplateService.resolveSettings(state.metadata(), "my-template");
        assertThat(settings.get("index.number_of_replicas"), equalTo("2"));
        assertThat(settings.get("index.blocks.write"), equalTo("false"));
        assertThat(settings.get("index.blocks.read"), equalTo("true"));
        assertThat(settings.get("index.number_of_shards"), equalTo("3"));
    }

    public void testResolveAliases() throws Exception {
        final MetadataIndexTemplateService service = getMetadataIndexTemplateService();
        ClusterState state = ClusterState.EMPTY_STATE;

        Map<String, AliasMetadata> a1 = new HashMap<>();
        a1.put("foo", AliasMetadata.newAliasMetadataBuilder("foo").build());
        Map<String, AliasMetadata> a2 = new HashMap<>();
        a2.put("bar", AliasMetadata.newAliasMetadataBuilder("bar").build());
        Map<String, AliasMetadata> a3 = new HashMap<>();
        a3.put("eggplant", AliasMetadata.newAliasMetadataBuilder("eggplant").build());
        a3.put("baz", AliasMetadata.newAliasMetadataBuilder("baz").build());

        ComponentTemplate ct1 = new ComponentTemplate(new Template(null, null, a1), null, null);
        ComponentTemplate ct2 = new ComponentTemplate(new Template(null, null, a2), null, null);
        state = service.addComponentTemplate(state, true, "ct_high", ct1);
        state = service.addComponentTemplate(state, true, "ct_low", ct2);
        IndexTemplateV2 it = new IndexTemplateV2(List.of("i*"),
            new Template(null, null, a3),
            List.of("ct_low", "ct_high"), 0L, 1L, null, null);
        state = service.addIndexTemplateV2(state, true, "my-template", it);

        List<Map<String, AliasMetadata>> resolvedAliases = MetadataIndexTemplateService.resolveAliases(state.metadata(), "my-template");

        // These should be order of precedence, so the index template (a3), then ct_high (a1), then ct_low (a2)
        assertThat(resolvedAliases, equalTo(List.of(a3, a1, a2)));
    }

    public void testAddInvalidTemplate() throws Exception {
        IndexTemplateV2 template = new IndexTemplateV2(Collections.singletonList("a"), null,
            Arrays.asList("good", "bad"), null, null, null);
        ComponentTemplate ct = new ComponentTemplate(new Template(Settings.EMPTY, null, null), null, null);

        final MetadataIndexTemplateService service = getMetadataIndexTemplateService();
        CountDownLatch ctLatch = new CountDownLatch(1);
        service.putComponentTemplate("api", randomBoolean(), "good", TimeValue.timeValueSeconds(5), ct,
            ActionListener.wrap(r -> ctLatch.countDown(), e -> {
                logger.error("unexpected error", e);
                fail("unexpected error");
            }));
        ctLatch.await(5, TimeUnit.SECONDS);
        InvalidIndexTemplateException e = expectThrows(InvalidIndexTemplateException.class,
            () -> {
                CountDownLatch latch = new CountDownLatch(1);
                AtomicReference<Exception> err = new AtomicReference<>();
                service.putIndexTemplateV2("api", randomBoolean(), "template", TimeValue.timeValueSeconds(30), template,
                    ActionListener.wrap(r -> fail("should have failed!"), exception -> {
                        err.set(exception);
                        latch.countDown();
                    }));
                latch.await(5, TimeUnit.SECONDS);
                if (err.get() != null) {
                    throw err.get();
                }
            });

        assertThat(e.name(), equalTo("template"));
        assertThat(e.getMessage(), containsString("index template [template] specifies " +
            "component templates [bad] that do not exist"));
    }

    public void testRemoveComponentTemplateInUse() throws Exception {
        IndexTemplateV2 template = new IndexTemplateV2(Collections.singletonList("a"), null,
            Collections.singletonList("ct"), null, null, null);
        ComponentTemplate ct = new ComponentTemplate(new Template(null, new CompressedXContent("{}"), null), null, null);

        final MetadataIndexTemplateService service = getMetadataIndexTemplateService();
        CountDownLatch ctLatch = new CountDownLatch(1);
        service.putComponentTemplate("api", false, "ct", TimeValue.timeValueSeconds(5), ct,
            ActionListener.wrap(r -> ctLatch.countDown(), e -> fail("unexpected error")));
        ctLatch.await(5, TimeUnit.SECONDS);

        CountDownLatch latch = new CountDownLatch(1);
        service.putIndexTemplateV2("api", false, "template", TimeValue.timeValueSeconds(30), template,
            ActionListener.wrap(r -> latch.countDown(), e -> fail("unexpected error")));
        latch.await(5, TimeUnit.SECONDS);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> {
                AtomicReference<Exception> err = new AtomicReference<>();
                CountDownLatch errLatch = new CountDownLatch(1);
                service.removeComponentTemplate("c*", TimeValue.timeValueSeconds(30),
                    ActionListener.wrap(r -> fail("should have failed!"), exception -> {
                        err.set(exception);
                        errLatch.countDown();
                    }));
                errLatch.await(5, TimeUnit.SECONDS);
                if (err.get() != null) {
                    throw err.get();
                }
            });

        assertThat(e.getMessage(),
            containsString("component templates [ct] cannot be removed as they are still in use by index templates [template]"));
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

    @SuppressWarnings("unchecked")
    public static void assertTemplatesEqual(IndexTemplateV2 actual, IndexTemplateV2 expected) {
        IndexTemplateV2 actualNoTemplate = new IndexTemplateV2(actual.indexPatterns(), null,
            actual.composedOf(), actual.priority(), actual.version(), actual.metadata(), actual.getDataStreamTemplate());
        IndexTemplateV2 expectedNoTemplate = new IndexTemplateV2(expected.indexPatterns(), null,
            expected.composedOf(), expected.priority(), expected.version(), expected.metadata(), expected.getDataStreamTemplate());

        assertThat(actualNoTemplate, equalTo(expectedNoTemplate));
        Template actualTemplate = actual.template();
        Template expectedTemplate = expected.template();

        assertThat("expected both templates to have either a template or no template",
            Objects.nonNull(actualTemplate), equalTo(Objects.nonNull(expectedTemplate)));

        if (actualTemplate != null) {
            assertThat(actualTemplate.settings(), equalTo(expectedTemplate.settings()));
            assertThat(actualTemplate.aliases(), equalTo(expectedTemplate.aliases()));
            assertThat("expected both templates to have either mappings or no mappings",
                Objects.nonNull(actualTemplate.mappings()), equalTo(Objects.nonNull(expectedTemplate.mappings())));

            if (actualTemplate.mappings() != null) {
                Map<String, Object> actualMappings;
                Map<String, Object> expectedMappings;
                try (XContentParser parser = XContentType.JSON.xContent()
                    .createParser(new NamedXContentRegistry(List.of()), LoggingDeprecationHandler.INSTANCE,
                        actualTemplate.mappings().string())) {
                    actualMappings = parser.map();
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
                try (XContentParser parser = XContentType.JSON.xContent()
                    .createParser(new NamedXContentRegistry(List.of()), LoggingDeprecationHandler.INSTANCE,
                        expectedTemplate.mappings().string())) {
                    expectedMappings = parser.map();
                } catch (IOException e) {
                    throw new AssertionError(e);
                }

                if (actualMappings.size() == 1 && actualMappings.containsKey(MapperService.SINGLE_MAPPING_NAME)) {
                    actualMappings = (Map<String, Object>) actualMappings.get(MapperService.SINGLE_MAPPING_NAME);
                }

                if (expectedMappings.size() == 1 && expectedMappings.containsKey(MapperService.SINGLE_MAPPING_NAME)) {
                    expectedMappings = (Map<String, Object>) expectedMappings.get(MapperService.SINGLE_MAPPING_NAME);
                }

                assertThat(actualMappings, equalTo(expectedMappings));
            }
        }
    }
}
