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
package org.elasticsearch.client;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.elasticsearch.client.RequestConvertersTests.assertToXContentBody;
import static org.elasticsearch.client.RequestConvertersTests.getAndExistsTest;
import static org.elasticsearch.client.RequestConvertersTests.randomIndicesNames;
import static org.elasticsearch.client.RequestConvertersTests.setRandomHumanReadable;
import static org.elasticsearch.client.RequestConvertersTests.setRandomIncludeDefaults;
import static org.elasticsearch.client.RequestConvertersTests.setRandomIndicesOptions;
import static org.elasticsearch.client.RequestConvertersTests.setRandomLocal;
import static org.elasticsearch.client.RequestConvertersTests.setRandomMasterTimeout;
import static org.elasticsearch.client.RequestConvertersTests.setRandomRefreshPolicy;
import static org.elasticsearch.client.RequestConvertersTests.setRandomTimeout;
import static org.elasticsearch.client.RequestConvertersTests.setRandomVersion;
import static org.elasticsearch.client.RequestConvertersTests.setRandomVersionType;
import static org.elasticsearch.client.RequestConvertersTests.setRandomWaitForActiveShards;
import static org.elasticsearch.index.RandomCreateIndexGenerator.randomCreateIndexRequest;
import static org.elasticsearch.index.alias.RandomAliasActionsGenerator.randomAliasAction;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndicesRequestConvertersTests extends ESTestCase {

    public void testExistsAlias() {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 5);
        getAliasesRequest.indices(indices);
        // the HEAD endpoint requires at least an alias or an index
        boolean hasIndices = indices != null && indices.length > 0;
        String[] aliases;
        if (hasIndices) {
            aliases = randomBoolean() ? null : randomIndicesNames(0, 5);
        } else {
            aliases = randomIndicesNames(1, 5);
        }
        getAliasesRequest.aliases(aliases);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomLocal(getAliasesRequest, expectedParams);
        setRandomIndicesOptions(getAliasesRequest::indicesOptions, getAliasesRequest::indicesOptions, expectedParams);

        Request request = IndicesRequestConverters.existsAlias(getAliasesRequest);
        StringJoiner expectedEndpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            expectedEndpoint.add(String.join(",", indices));
        }
        expectedEndpoint.add("_alias");
        if (aliases != null && aliases.length > 0) {
            expectedEndpoint.add(String.join(",", aliases));
        }
        assertEquals(HttpHead.METHOD_NAME, request.getMethod());
        assertEquals(expectedEndpoint.toString(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
    }

    public void testExistsAliasNoAliasNoIndex() {
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                    () -> IndicesRequestConverters.existsAlias(getAliasesRequest));
            assertEquals("existsAlias requires at least an alias or an index", iae.getMessage());
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest((String[]) null);
            getAliasesRequest.indices((String[]) null);
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                    () -> IndicesRequestConverters.existsAlias(getAliasesRequest));
            assertEquals("existsAlias requires at least an alias or an index", iae.getMessage());
        }
    }

    public void testExplain() throws IOException {
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);

        ExplainRequest explainRequest = new ExplainRequest(index, type, id);
        explainRequest.query(QueryBuilders.termQuery(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10)));

        Map<String, String> expectedParams = new HashMap<>();

        if (randomBoolean()) {
            String routing = randomAlphaOfLengthBetween(3, 10);
            explainRequest.routing(routing);
            expectedParams.put("routing", routing);
        }
        if (randomBoolean()) {
            String preference = randomAlphaOfLengthBetween(3, 10);
            explainRequest.preference(preference);
            expectedParams.put("preference", preference);
        }
        if (randomBoolean()) {
            String[] storedFields = generateRandomStringArray(10, 5, false, false);
            String storedFieldsParams = RequestConvertersTests.randomFields(storedFields);
            explainRequest.storedFields(storedFields);
            expectedParams.put("stored_fields", storedFieldsParams);
        }
        if (randomBoolean()) {
            RequestConvertersTests.randomizeFetchSourceContextParams(explainRequest::fetchSourceContext, expectedParams);
        }

        Request request = RequestConverters.explain(explainRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        endpoint.add(index)
            .add(type)
            .add(id)
            .add("_explain");

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(explainRequest, request.getEntity());
    }

    public void testSplit() throws IOException {
        RequestConvertersTests.resizeTest(ResizeType.SPLIT, IndicesRequestConverters::split);
    }

    public void testSplitWrongResizeType() {
        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SHRINK);
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> IndicesRequestConverters.split(resizeRequest));
        assertEquals("Wrong resize type [SHRINK] for indices split request", iae.getMessage());
    }

    public void testShrinkWrongResizeType() {
        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> IndicesRequestConverters.shrink(resizeRequest));
        assertEquals("Wrong resize type [SPLIT] for indices shrink request", iae.getMessage());
    }

    public void testShrink() throws IOException {
        RequestConvertersTests.resizeTest(ResizeType.SHRINK, IndicesRequestConverters::shrink);
    }

    public void testRollover() throws IOException {
        RolloverRequest rolloverRequest = new RolloverRequest(randomAlphaOfLengthBetween(3, 10),
                randomBoolean() ? null : randomAlphaOfLengthBetween(3, 10));
        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(rolloverRequest::timeout, rolloverRequest.timeout(), expectedParams);
        setRandomMasterTimeout(rolloverRequest, expectedParams);
        if (randomBoolean()) {
            rolloverRequest.dryRun(randomBoolean());
            if (rolloverRequest.isDryRun()) {
                expectedParams.put("dry_run", "true");
            }
        }
        if (randomBoolean()) {
            rolloverRequest.addMaxIndexAgeCondition(new TimeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            String type = randomAlphaOfLengthBetween(3, 10);
            rolloverRequest.getCreateIndexRequest().mapping(type, RandomCreateIndexGenerator.randomMapping(type));
        }
        if (randomBoolean()) {
            RandomCreateIndexGenerator.randomAliases(rolloverRequest.getCreateIndexRequest());
        }
        if (randomBoolean()) {
            rolloverRequest.getCreateIndexRequest().settings(RandomCreateIndexGenerator.randomIndexSettings());
        }
        setRandomWaitForActiveShards(rolloverRequest.getCreateIndexRequest()::waitForActiveShards, expectedParams);

        Request request = IndicesRequestConverters.rollover(rolloverRequest);
        if (rolloverRequest.getNewIndexName() == null) {
            assertEquals("/" + rolloverRequest.getAlias() + "/_rollover", request.getEndpoint());
        } else {
            assertEquals("/" + rolloverRequest.getAlias() + "/_rollover/" + rolloverRequest.getNewIndexName(), request.getEndpoint());
        }
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertToXContentBody(rolloverRequest, request.getEntity());
        assertEquals(expectedParams, request.getParameters());
    }

    public void testGetAlias() {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest();

        Map<String, String> expectedParams = new HashMap<>();
        setRandomLocal(getAliasesRequest, expectedParams);
        setRandomIndicesOptions(getAliasesRequest::indicesOptions, getAliasesRequest::indicesOptions, expectedParams);

        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 2);
        String[] aliases = randomBoolean() ? null : randomIndicesNames(0, 2);
        getAliasesRequest.indices(indices);
        getAliasesRequest.aliases(aliases);

        Request request = IndicesRequestConverters.getAlias(getAliasesRequest);
        StringJoiner expectedEndpoint = new StringJoiner("/", "/", "");

        if (false == CollectionUtils.isEmpty(indices)) {
            expectedEndpoint.add(String.join(",", indices));
        }
        expectedEndpoint.add("_alias");

        if (false == CollectionUtils.isEmpty(aliases)) {
            expectedEndpoint.add(String.join(",", aliases));
        }

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals(expectedEndpoint.toString(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
    }

    public void testIndexPutSettings() throws IOException {
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 2);
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indices);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(updateSettingsRequest, expectedParams);
        setRandomTimeout(updateSettingsRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomIndicesOptions(updateSettingsRequest::indicesOptions, updateSettingsRequest::indicesOptions, expectedParams);
        if (randomBoolean()) {
            updateSettingsRequest.setPreserveExisting(randomBoolean());
            if (updateSettingsRequest.isPreserveExisting()) {
                expectedParams.put("preserve_existing", "true");
            }
        }

        Request request = IndicesRequestConverters.indexPutSettings(updateSettingsRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_settings");
        assertThat(endpoint.toString(), CoreMatchers.equalTo(request.getEndpoint()));
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertToXContentBody(updateSettingsRequest, request.getEntity());
        assertEquals(expectedParams, request.getParameters());
    }

    public void testPutTemplateRequest() throws Exception {
        Map<String, String> names = new HashMap<>();
        names.put("log", "log");
        names.put("template#1", "template%231");
        names.put("-#template", "-%23template");
        names.put("foo^bar", "foo%5Ebar");

        PutIndexTemplateRequest putTemplateRequest = new PutIndexTemplateRequest().name(randomFrom(names.keySet()))
                .patterns(Arrays.asList(generateRandomStringArray(20, 100, false, false)));
        if (randomBoolean()) {
            putTemplateRequest.order(randomInt());
        }
        if (randomBoolean()) {
            putTemplateRequest.version(randomInt());
        }
        if (randomBoolean()) {
            putTemplateRequest.settings(Settings.builder().put("setting-" + randomInt(), randomTimeValue()));
        }
        if (randomBoolean()) {
            putTemplateRequest.mapping("doc-" + randomInt(), "field-" + randomInt(), "type=" + randomFrom("text", "keyword"));
        }
        if (randomBoolean()) {
            putTemplateRequest.alias(new Alias("alias-" + randomInt()));
        }
        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            expectedParams.put("create", Boolean.TRUE.toString());
            putTemplateRequest.create(true);
        }
        if (randomBoolean()) {
            String cause = randomUnicodeOfCodepointLengthBetween(1, 50);
            putTemplateRequest.cause(cause);
            expectedParams.put("cause", cause);
        }
        setRandomMasterTimeout(putTemplateRequest, expectedParams);
        Request request = IndicesRequestConverters.putTemplate(putTemplateRequest);
        assertThat(request.getEndpoint(), CoreMatchers.equalTo("/_template/" + names.get(putTemplateRequest.name())));
        assertThat(request.getParameters(), CoreMatchers.equalTo(expectedParams));
        assertToXContentBody(putTemplateRequest, request.getEntity());
    }

    public void testValidateQuery() throws Exception {
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 5);
        String[] types = randomBoolean() ? generateRandomStringArray(5, 5, false, false) : null;
        ValidateQueryRequest validateQueryRequest;
        if (randomBoolean()) {
            validateQueryRequest = new ValidateQueryRequest(indices);
        } else {
            validateQueryRequest = new ValidateQueryRequest();
            validateQueryRequest.indices(indices);
        }
        validateQueryRequest.types(types);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(validateQueryRequest::indicesOptions, validateQueryRequest::indicesOptions, expectedParams);
        validateQueryRequest.explain(randomBoolean());
        validateQueryRequest.rewrite(randomBoolean());
        validateQueryRequest.allShards(randomBoolean());
        expectedParams.put("explain", Boolean.toString(validateQueryRequest.explain()));
        expectedParams.put("rewrite", Boolean.toString(validateQueryRequest.rewrite()));
        expectedParams.put("all_shards", Boolean.toString(validateQueryRequest.allShards()));
        Request request = IndicesRequestConverters.validateQuery(validateQueryRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
            if (types != null && types.length > 0) {
                endpoint.add(String.join(",", types));
            }
        }
        endpoint.add("_validate/query");
        assertThat(request.getEndpoint(), CoreMatchers.equalTo(endpoint.toString()));
        assertThat(request.getParameters(), CoreMatchers.equalTo(expectedParams));
        assertToXContentBody(validateQueryRequest, request.getEntity());
        assertThat(request.getMethod(), CoreMatchers.equalTo(HttpGet.METHOD_NAME));
    }

    public void testGetTemplateRequest() throws Exception {
        Map<String, String> encodes = new HashMap<>();
        encodes.put("log", "log");
        encodes.put("1", "1");
        encodes.put("template#1", "template%231");
        encodes.put("template-*", "template-*");
        encodes.put("foo^bar", "foo%5Ebar");
        List<String> names = randomSubsetOf(1, encodes.keySet());
        GetIndexTemplatesRequest getTemplatesRequest = new GetIndexTemplatesRequest().names(names.toArray(new String[0]));
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(getTemplatesRequest, expectedParams);
        setRandomLocal(getTemplatesRequest, expectedParams);
        Request request = IndicesRequestConverters.getTemplates(getTemplatesRequest);
        assertThat(request.getEndpoint(), CoreMatchers.equalTo("/_template/" + names.stream().map(encodes::get)
            .collect(Collectors.joining(","))));
        assertThat(request.getParameters(), CoreMatchers.equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
    }

    public void testAnalyzeRequest() throws Exception {
        AnalyzeRequest indexAnalyzeRequest = new AnalyzeRequest()
            .text("Here is some text")
            .index("test_index")
            .analyzer("test_analyzer");

        Request request = IndicesRequestConverters.analyze(indexAnalyzeRequest);
        assertThat(request.getEndpoint(), CoreMatchers.equalTo("/test_index/_analyze"));
        assertToXContentBody(indexAnalyzeRequest, request.getEntity());

        AnalyzeRequest analyzeRequest = new AnalyzeRequest()
            .text("more text")
            .analyzer("test_analyzer");
        assertThat(IndicesRequestConverters.analyze(analyzeRequest).getEndpoint(), CoreMatchers.equalTo("/_analyze"));
    }

    public void testPutMapping() throws IOException {
        PutMappingRequest putMappingRequest = new PutMappingRequest();

        String[] indices = randomIndicesNames(0, 5);
        putMappingRequest.indices(indices);

        String type = randomAlphaOfLengthBetween(3, 10);
        putMappingRequest.type(type);

        Map<String, String> expectedParams = new HashMap<>();

        setRandomTimeout(putMappingRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(putMappingRequest, expectedParams);

        Request request = IndicesRequestConverters.putMapping(putMappingRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");
        endpoint.add(type);
        assertEquals(endpoint.toString(), request.getEndpoint());

        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertToXContentBody(putMappingRequest, request.getEntity());
    }

    public void testGetMapping() throws IOException {
        GetMappingsRequest getMappingRequest = new GetMappingsRequest();

        String[] indices = Strings.EMPTY_ARRAY;
        if (randomBoolean()) {
            indices = randomIndicesNames(0, 5);
            getMappingRequest.indices(indices);
        } else if (randomBoolean()) {
            getMappingRequest.indices((String[]) null);
        }

        String type = null;
        if (randomBoolean()) {
            type = randomAlphaOfLengthBetween(3, 10);
            getMappingRequest.types(type);
        } else if (randomBoolean()) {
            getMappingRequest.types((String[]) null);
        }

        Map<String, String> expectedParams = new HashMap<>();

        setRandomIndicesOptions(getMappingRequest::indicesOptions, getMappingRequest::indicesOptions, expectedParams);
        setRandomMasterTimeout(getMappingRequest, expectedParams);
        setRandomLocal(getMappingRequest, expectedParams);

        Request request = IndicesRequestConverters.getMappings(getMappingRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");
        if (type != null) {
            endpoint.add(type);
        }
        assertThat(endpoint.toString(), CoreMatchers.equalTo(request.getEndpoint()));

        assertThat(expectedParams, CoreMatchers.equalTo(request.getParameters()));
        assertThat(HttpGet.METHOD_NAME, CoreMatchers.equalTo(request.getMethod()));
    }

    public void testGetFieldMapping() throws IOException {
        GetFieldMappingsRequest getFieldMappingsRequest = new GetFieldMappingsRequest();

        String[] indices = Strings.EMPTY_ARRAY;
        if (randomBoolean()) {
            indices = randomIndicesNames(0, 5);
            getFieldMappingsRequest.indices(indices);
        } else if (randomBoolean()) {
            getFieldMappingsRequest.indices((String[]) null);
        }

        String type = null;
        if (randomBoolean()) {
            type = randomAlphaOfLengthBetween(3, 10);
            getFieldMappingsRequest.types(type);
        } else if (randomBoolean()) {
            getFieldMappingsRequest.types((String[]) null);
        }

        String[] fields = null;
        if (randomBoolean()) {
            fields = new String[randomIntBetween(1, 5)];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = randomAlphaOfLengthBetween(3, 10);
            }
            getFieldMappingsRequest.fields(fields);
        } else if (randomBoolean()) {
            getFieldMappingsRequest.fields((String[]) null);
        }

        Map<String, String> expectedParams = new HashMap<>();

        setRandomIndicesOptions(getFieldMappingsRequest::indicesOptions, getFieldMappingsRequest::indicesOptions, expectedParams);
        setRandomLocal(getFieldMappingsRequest::local, expectedParams);

        Request request = IndicesRequestConverters.getFieldMapping(getFieldMappingsRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");
        if (type != null) {
            endpoint.add(type);
        }
        endpoint.add("field");
        if (fields != null) {
            endpoint.add(String.join(",", fields));
        }
        assertThat(endpoint.toString(), CoreMatchers.equalTo(request.getEndpoint()));

        assertThat(expectedParams, CoreMatchers.equalTo(request.getParameters()));
        assertThat(HttpGet.METHOD_NAME, CoreMatchers.equalTo(request.getMethod()));
    }

    public void testDeleteIndex() {
        String[] indices = randomIndicesNames(0, 5);
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(deleteIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(deleteIndexRequest, expectedParams);

        setRandomIndicesOptions(deleteIndexRequest::indicesOptions, deleteIndexRequest::indicesOptions, expectedParams);

        Request request = IndicesRequestConverters.deleteIndex(deleteIndexRequest);
        assertEquals("/" + String.join(",", indices), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertNull(request.getEntity());
    }

    public void testGetSettings() throws IOException {
        String[] indicesUnderTest = randomBoolean() ? null : randomIndicesNames(0, 5);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(indicesUnderTest);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(getSettingsRequest, expectedParams);
        setRandomIndicesOptions(getSettingsRequest::indicesOptions, getSettingsRequest::indicesOptions, expectedParams);

        setRandomLocal(getSettingsRequest, expectedParams);

        if (randomBoolean()) {
            // the request object will not have include_defaults present unless it is set to
            // true
            getSettingsRequest.includeDefaults(randomBoolean());
            if (getSettingsRequest.includeDefaults()) {
                expectedParams.put("include_defaults", Boolean.toString(true));
            }
        }

        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indicesUnderTest != null && indicesUnderTest.length > 0) {
            endpoint.add(String.join(",", indicesUnderTest));
        }
        endpoint.add("_settings");

        if (randomBoolean()) {
            String[] names = randomBoolean() ? null : new String[randomIntBetween(0, 3)];
            if (names != null) {
                for (int x = 0; x < names.length; x++) {
                    names[x] = randomAlphaOfLengthBetween(3, 10);
                }
            }
            getSettingsRequest.names(names);
            if (names != null && names.length > 0) {
                endpoint.add(String.join(",", names));
            }
        }

        Request request = IndicesRequestConverters.getSettings(getSettingsRequest);

        assertThat(endpoint.toString(), CoreMatchers.equalTo(request.getEndpoint()));
        assertThat(request.getParameters(), CoreMatchers.equalTo(expectedParams));
        assertThat(request.getMethod(), CoreMatchers.equalTo(HttpGet.METHOD_NAME));
        assertThat(request.getEntity(), nullValue());
    }

    public void testGetIndex() throws IOException {
        String[] indicesUnderTest = randomBoolean() ? null : randomIndicesNames(0, 5);

        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(indicesUnderTest);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(getIndexRequest, expectedParams);
        setRandomIndicesOptions(getIndexRequest::indicesOptions, getIndexRequest::indicesOptions, expectedParams);
        setRandomLocal(getIndexRequest, expectedParams);
        setRandomHumanReadable(getIndexRequest, expectedParams);

        if (randomBoolean()) {
            // the request object will not have include_defaults present unless it is set to
            // true
            getIndexRequest.includeDefaults(randomBoolean());
            if (getIndexRequest.includeDefaults()) {
                expectedParams.put("include_defaults", Boolean.toString(true));
            }
        }

        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indicesUnderTest != null && indicesUnderTest.length > 0) {
            endpoint.add(String.join(",", indicesUnderTest));
        }

        Request request = IndicesRequestConverters.getIndex(getIndexRequest);

        assertThat(endpoint.toString(), CoreMatchers.equalTo(request.getEndpoint()));
        assertThat(request.getParameters(), CoreMatchers.equalTo(expectedParams));
        assertThat(request.getMethod(), CoreMatchers.equalTo(HttpGet.METHOD_NAME));
        assertThat(request.getEntity(), nullValue());
    }

    public void testDeleteIndexEmptyIndices() {
        String[] indices = randomBoolean() ? null : Strings.EMPTY_ARRAY;
        ActionRequestValidationException validationException = new DeleteIndexRequest(indices).validate();
        assertNotNull(validationException);
    }

    public void testOpenIndex() {
        String[] indices = randomIndicesNames(1, 5);
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(indices);
        openIndexRequest.indices(indices);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(openIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(openIndexRequest, expectedParams);
        setRandomIndicesOptions(openIndexRequest::indicesOptions, openIndexRequest::indicesOptions, expectedParams);
        setRandomWaitForActiveShards(openIndexRequest::waitForActiveShards, expectedParams);

        Request request = IndicesRequestConverters.openIndex(openIndexRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "").add(String.join(",", indices)).add("_open");
        assertThat(endpoint.toString(), CoreMatchers.equalTo(request.getEndpoint()));
        assertThat(expectedParams, CoreMatchers.equalTo(request.getParameters()));
        assertThat(request.getMethod(), CoreMatchers.equalTo(HttpPost.METHOD_NAME));
        assertThat(request.getEntity(), nullValue());
    }

    public void testOpenIndexEmptyIndices() {
        String[] indices = randomBoolean() ? null : Strings.EMPTY_ARRAY;
        ActionRequestValidationException validationException = new OpenIndexRequest(indices).validate();
        assertNotNull(validationException);
    }

    public void testCloseIndex() {
        String[] indices = randomIndicesNames(1, 5);
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(closeIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(closeIndexRequest, expectedParams);
        setRandomIndicesOptions(closeIndexRequest::indicesOptions, closeIndexRequest::indicesOptions, expectedParams);

        Request request = IndicesRequestConverters.closeIndex(closeIndexRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "").add(String.join(",", indices)).add("_close");
        assertThat(endpoint.toString(), CoreMatchers.equalTo(request.getEndpoint()));
        assertThat(expectedParams, CoreMatchers.equalTo(request.getParameters()));
        assertThat(request.getMethod(), CoreMatchers.equalTo(HttpPost.METHOD_NAME));
        assertThat(request.getEntity(), nullValue());
    }

    public void testCloseIndexEmptyIndices() {
        String[] indices = randomBoolean() ? null : Strings.EMPTY_ARRAY;
        ActionRequestValidationException validationException = new CloseIndexRequest(indices).validate();
        assertNotNull(validationException);
    }

    public void testIndex() throws IOException {
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        IndexRequest indexRequest = new IndexRequest(index, type);

        String id = randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null;
        indexRequest.id(id);

        Map<String, String> expectedParams = new HashMap<>();

        String method = HttpPost.METHOD_NAME;
        if (id != null) {
            method = HttpPut.METHOD_NAME;
            if (randomBoolean()) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE);
            }
        }

        setRandomTimeout(indexRequest::timeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        setRandomRefreshPolicy(indexRequest::setRefreshPolicy, expectedParams);

        // There is some logic around _create endpoint and version/version type
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            indexRequest.version(randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED));
            expectedParams.put("version", Long.toString(Versions.MATCH_DELETED));
        } else {
            setRandomVersion(indexRequest, expectedParams);
            setRandomVersionType(indexRequest::versionType, expectedParams);
        }

        if (frequently()) {
            if (randomBoolean()) {
                String routing = randomAlphaOfLengthBetween(3, 10);
                indexRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
            if (randomBoolean()) {
                String pipeline = randomAlphaOfLengthBetween(3, 10);
                indexRequest.setPipeline(pipeline);
                expectedParams.put("pipeline", pipeline);
            }
        }

        XContentType xContentType = randomFrom(XContentType.values());
        int nbFields = randomIntBetween(0, 10);
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            for (int i = 0; i < nbFields; i++) {
                builder.field("field_" + i, i);
            }
            builder.endObject();
            indexRequest.source(builder);
        }

        Request request = IndicesRequestConverters.index(indexRequest);
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            assertEquals("/" + index + "/" + type + "/" + id + "/_create", request.getEndpoint());
        } else if (id != null) {
            assertEquals("/" + index + "/" + type + "/" + id, request.getEndpoint());
        } else {
            assertEquals("/" + index + "/" + type, request.getEndpoint());
        }
        assertEquals(expectedParams, request.getParameters());
        assertEquals(method, request.getMethod());

        HttpEntity entity = request.getEntity();
        assertTrue(entity instanceof ByteArrayEntity);
        assertEquals(indexRequest.getContentType().mediaTypeWithoutParameters(), entity.getContentType().getValue());
        try (XContentParser parser = createParser(xContentType.xContent(), entity.getContent())) {
            assertEquals(nbFields, parser.map().size());
        }
    }

    public void testRefresh() {
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 5);
        RefreshRequest refreshRequest;
        if (randomBoolean()) {
            refreshRequest = new RefreshRequest(indices);
        } else {
            refreshRequest = new RefreshRequest();
            refreshRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(refreshRequest::indicesOptions, refreshRequest::indicesOptions, expectedParams);
        Request request = IndicesRequestConverters.refresh(refreshRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_refresh");
        assertThat(request.getEndpoint(), CoreMatchers.equalTo(endpoint.toString()));
        assertThat(request.getParameters(), CoreMatchers.equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
        assertThat(request.getMethod(), CoreMatchers.equalTo(HttpPost.METHOD_NAME));
    }

    public void testFlush() {
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 5);
        FlushRequest flushRequest;
        if (randomBoolean()) {
            flushRequest = new FlushRequest(indices);
        } else {
            flushRequest = new FlushRequest();
            flushRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(flushRequest::indicesOptions, flushRequest::indicesOptions, expectedParams);
        if (randomBoolean()) {
            flushRequest.force(randomBoolean());
        }
        expectedParams.put("force", Boolean.toString(flushRequest.force()));
        if (randomBoolean()) {
            flushRequest.waitIfOngoing(randomBoolean());
        }
        expectedParams.put("wait_if_ongoing", Boolean.toString(flushRequest.waitIfOngoing()));

        Request request = IndicesRequestConverters.flush(flushRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_flush");
        assertThat(request.getEndpoint(), CoreMatchers.equalTo(endpoint.toString()));
        assertThat(request.getParameters(), CoreMatchers.equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
        assertThat(request.getMethod(), CoreMatchers.equalTo(HttpPost.METHOD_NAME));
    }

    public void testSyncedFlush() {
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 5);
        SyncedFlushRequest syncedFlushRequest;
        if (randomBoolean()) {
            syncedFlushRequest = new SyncedFlushRequest(indices);
        } else {
            syncedFlushRequest = new SyncedFlushRequest();
            syncedFlushRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(syncedFlushRequest::indicesOptions, syncedFlushRequest::indicesOptions, expectedParams);
        Request request = IndicesRequestConverters.flushSynced(syncedFlushRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
                endpoint.add(String.join(",", indices));
            }
        endpoint.add("_flush/synced");
        assertThat(request.getEndpoint(), CoreMatchers.equalTo(endpoint.toString()));
        assertThat(request.getParameters(), CoreMatchers.equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
        assertThat(request.getMethod(), CoreMatchers.equalTo(HttpPost.METHOD_NAME));
    }

    public void testForceMerge() {
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 5);
        ForceMergeRequest forceMergeRequest;
        if (randomBoolean()) {
            forceMergeRequest = new ForceMergeRequest(indices);
        } else {
            forceMergeRequest = new ForceMergeRequest();
            forceMergeRequest.indices(indices);
        }

        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(forceMergeRequest::indicesOptions, forceMergeRequest::indicesOptions, expectedParams);
        if (randomBoolean()) {
            forceMergeRequest.maxNumSegments(randomInt());
        }
        expectedParams.put("max_num_segments", Integer.toString(forceMergeRequest.maxNumSegments()));
        if (randomBoolean()) {
            forceMergeRequest.onlyExpungeDeletes(randomBoolean());
        }
        expectedParams.put("only_expunge_deletes", Boolean.toString(forceMergeRequest.onlyExpungeDeletes()));
        if (randomBoolean()) {
            forceMergeRequest.flush(randomBoolean());
        }
        expectedParams.put("flush", Boolean.toString(forceMergeRequest.flush()));

        Request request = IndicesRequestConverters.forceMerge(forceMergeRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_forcemerge");
        assertThat(request.getEndpoint(), CoreMatchers.equalTo(endpoint.toString()));
        assertThat(request.getParameters(), CoreMatchers.equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
        assertThat(request.getMethod(), CoreMatchers.equalTo(HttpPost.METHOD_NAME));
    }

    public void testClearCache() {
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 5);
        ClearIndicesCacheRequest clearIndicesCacheRequest;
        if (randomBoolean()) {
            clearIndicesCacheRequest = new ClearIndicesCacheRequest(indices);
        } else {
            clearIndicesCacheRequest = new ClearIndicesCacheRequest();
            clearIndicesCacheRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(clearIndicesCacheRequest::indicesOptions, clearIndicesCacheRequest::indicesOptions, expectedParams);
        if (randomBoolean()) {
            clearIndicesCacheRequest.queryCache(randomBoolean());
        }
        expectedParams.put("query", Boolean.toString(clearIndicesCacheRequest.queryCache()));
        if (randomBoolean()) {
            clearIndicesCacheRequest.fieldDataCache(randomBoolean());
        }
        expectedParams.put("fielddata", Boolean.toString(clearIndicesCacheRequest.fieldDataCache()));
        if (randomBoolean()) {
            clearIndicesCacheRequest.requestCache(randomBoolean());
        }
        expectedParams.put("request", Boolean.toString(clearIndicesCacheRequest.requestCache()));
        if (randomBoolean()) {
            clearIndicesCacheRequest.fields(randomIndicesNames(1, 5));
            expectedParams.put("fields", String.join(",", clearIndicesCacheRequest.fields()));
        }

        Request request = IndicesRequestConverters.clearCache(clearIndicesCacheRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_cache/clear");
        assertThat(request.getEndpoint(), CoreMatchers.equalTo(endpoint.toString()));
        assertThat(request.getParameters(), CoreMatchers.equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
        assertThat(request.getMethod(), CoreMatchers.equalTo(HttpPost.METHOD_NAME));
    }

    public void testGet() {
        getAndExistsTest(IndicesRequestConverters::get, HttpGet.METHOD_NAME);
    }
    
    public void testDelete() {
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);

        Map<String, String> expectedParams = new HashMap<>();

        setRandomTimeout(deleteRequest::timeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        setRandomRefreshPolicy(deleteRequest::setRefreshPolicy, expectedParams);
        setRandomVersion(deleteRequest, expectedParams);
        setRandomVersionType(deleteRequest::versionType, expectedParams);

        if (frequently()) {
            if (randomBoolean()) {
                String routing = randomAlphaOfLengthBetween(3, 10);
                deleteRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
        }

        Request request = IndicesRequestConverters.delete(deleteRequest);
        assertEquals("/" + index + "/" + type + "/" + id, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertNull(request.getEntity());
    }

    public void testIndicesExist() {
        String[] indices = randomIndicesNames(1, 10);

        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(indices);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(getIndexRequest::indicesOptions, getIndexRequest::indicesOptions, expectedParams);
        setRandomLocal(getIndexRequest, expectedParams);
        setRandomHumanReadable(getIndexRequest, expectedParams);
        setRandomIncludeDefaults(getIndexRequest, expectedParams);

        final Request request = IndicesRequestConverters.indicesExist(getIndexRequest);

        assertEquals(HttpHead.METHOD_NAME, request.getMethod());
        assertEquals("/" + String.join(",", indices), request.getEndpoint());
        assertThat(expectedParams, equalTo(request.getParameters()));
        assertNull(request.getEntity());
    }

    public void testIndicesExistEmptyIndices() {
        expectThrows(IllegalArgumentException.class, () -> IndicesRequestConverters.indicesExist(new GetIndexRequest()));
        expectThrows(IllegalArgumentException.class, () -> IndicesRequestConverters.indicesExist(new GetIndexRequest()
            .indices((String[]) null)));
    }
    
    public void testCreateIndex() throws IOException {
        CreateIndexRequest createIndexRequest = randomCreateIndexRequest();

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(createIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(createIndexRequest, expectedParams);
        setRandomWaitForActiveShards(createIndexRequest::waitForActiveShards, expectedParams);

        Request request = IndicesRequestConverters.createIndex(createIndexRequest);
        assertEquals("/" + createIndexRequest.index(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertToXContentBody(createIndexRequest, request.getEntity());
    }

    public void testCreateIndexNullIndex() {
        ActionRequestValidationException validationException = new CreateIndexRequest(null).validate();
        assertNotNull(validationException);
    }

    public void testUpdateAliases() throws IOException {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasAction = randomAliasAction();
        indicesAliasesRequest.addAliasAction(aliasAction);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(indicesAliasesRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(indicesAliasesRequest, expectedParams);

        Request request = IndicesRequestConverters.updateAliases(indicesAliasesRequest);
        assertEquals("/_aliases", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(indicesAliasesRequest, request.getEntity());
    }
}
