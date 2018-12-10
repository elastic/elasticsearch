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

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.ActionRequestValidationException;
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
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.elasticsearch.index.RandomCreateIndexGenerator.randomAliases;
import static org.elasticsearch.index.RandomCreateIndexGenerator.randomCreateIndexRequest;
import static org.elasticsearch.index.RandomCreateIndexGenerator.randomIndexSettings;
import static org.elasticsearch.index.alias.RandomAliasActionsGenerator.randomAliasAction;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndicesRequestConvertersTests extends ESTestCase {

    public void testAnalyzeRequest() throws Exception {
        AnalyzeRequest indexAnalyzeRequest = new AnalyzeRequest()
            .text("Here is some text")
            .index("test_index")
            .analyzer("test_analyzer");

        Request request = IndicesRequestConverters.analyze(indexAnalyzeRequest);
        assertThat(request.getEndpoint(), equalTo("/test_index/_analyze"));
        RequestConvertersTests.assertToXContentBody(indexAnalyzeRequest, request.getEntity());

        AnalyzeRequest analyzeRequest = new AnalyzeRequest()
            .text("more text")
            .analyzer("test_analyzer");
        assertThat(IndicesRequestConverters.analyze(analyzeRequest).getEndpoint(), equalTo("/_analyze"));
    }

    public void testIndicesExist() {
        String[] indices = RequestConvertersTests.randomIndicesNames(1, 10);

        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(indices);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(getIndexRequest::indicesOptions, getIndexRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomLocal(getIndexRequest, expectedParams);
        RequestConvertersTests.setRandomHumanReadable(getIndexRequest, expectedParams);
        RequestConvertersTests.setRandomIncludeDefaults(getIndexRequest, expectedParams);

        final Request request = IndicesRequestConverters.indicesExist(getIndexRequest);

        Assert.assertEquals(HttpHead.METHOD_NAME, request.getMethod());
        Assert.assertEquals("/" + String.join(",", indices), request.getEndpoint());
        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertNull(request.getEntity());
    }

    public void testIndicesExistEmptyIndices() {
        LuceneTestCase.expectThrows(IllegalArgumentException.class, ()
            -> IndicesRequestConverters.indicesExist(new GetIndexRequest()));
        LuceneTestCase.expectThrows(IllegalArgumentException.class, ()
            -> IndicesRequestConverters.indicesExist(new GetIndexRequest().indices((String[]) null)));
    }

    public void testCreateIndex() throws IOException {
        CreateIndexRequest createIndexRequest = randomCreateIndexRequest();

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(createIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(createIndexRequest, expectedParams);
        RequestConvertersTests.setRandomWaitForActiveShards(createIndexRequest::waitForActiveShards, expectedParams);

        Request request = IndicesRequestConverters.createIndex(createIndexRequest);
        Assert.assertEquals("/" + createIndexRequest.index(), request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(createIndexRequest, request.getEntity());
    }

    public void testCreateIndexNullIndex() {
        ActionRequestValidationException validationException = new CreateIndexRequest(null).validate();
        Assert.assertNotNull(validationException);
    }

    public void testUpdateAliases() throws IOException {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasAction = randomAliasAction();
        indicesAliasesRequest.addAliasAction(aliasAction);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(indicesAliasesRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(indicesAliasesRequest, expectedParams);

        Request request = IndicesRequestConverters.updateAliases(indicesAliasesRequest);
        Assert.assertEquals("/_aliases", request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        RequestConvertersTests.assertToXContentBody(indicesAliasesRequest, request.getEntity());
    }

    public void testPutMapping() throws IOException {
        PutMappingRequest putMappingRequest = new PutMappingRequest();

        String[] indices = RequestConvertersTests.randomIndicesNames(0, 5);
        putMappingRequest.indices(indices);

        String type = ESTestCase.randomAlphaOfLengthBetween(3, 10);
        putMappingRequest.type(type);

        Map<String, String> expectedParams = new HashMap<>();

        RequestConvertersTests.setRandomTimeout(putMappingRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(putMappingRequest, expectedParams);

        Request request = IndicesRequestConverters.putMapping(putMappingRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");
        endpoint.add(type);
        Assert.assertEquals(endpoint.toString(), request.getEndpoint());

        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(putMappingRequest, request.getEntity());
    }

    public void testGetMapping() throws IOException {
        GetMappingsRequest getMappingRequest = new GetMappingsRequest();

        String[] indices = Strings.EMPTY_ARRAY;
        if (ESTestCase.randomBoolean()) {
            indices = RequestConvertersTests.randomIndicesNames(0, 5);
            getMappingRequest.indices(indices);
        } else if (ESTestCase.randomBoolean()) {
            getMappingRequest.indices((String[]) null);
        }

        String type = null;
        if (ESTestCase.randomBoolean()) {
            type = ESTestCase.randomAlphaOfLengthBetween(3, 10);
            getMappingRequest.types(type);
        } else if (ESTestCase.randomBoolean()) {
            getMappingRequest.types((String[]) null);
        }

        Map<String, String> expectedParams = new HashMap<>();

        RequestConvertersTests.setRandomIndicesOptions(getMappingRequest::indicesOptions,
            getMappingRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(getMappingRequest, expectedParams);
        RequestConvertersTests.setRandomLocal(getMappingRequest, expectedParams);

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
        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));

        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertThat(HttpGet.METHOD_NAME, equalTo(request.getMethod()));
    }

    public void testGetFieldMapping() throws IOException {
        GetFieldMappingsRequest getFieldMappingsRequest = new GetFieldMappingsRequest();

        String[] indices = Strings.EMPTY_ARRAY;
        if (ESTestCase.randomBoolean()) {
            indices = RequestConvertersTests.randomIndicesNames(0, 5);
            getFieldMappingsRequest.indices(indices);
        } else if (ESTestCase.randomBoolean()) {
            getFieldMappingsRequest.indices((String[]) null);
        }

        String type = null;
        if (ESTestCase.randomBoolean()) {
            type = ESTestCase.randomAlphaOfLengthBetween(3, 10);
            getFieldMappingsRequest.types(type);
        } else if (ESTestCase.randomBoolean()) {
            getFieldMappingsRequest.types((String[]) null);
        }

        String[] fields = null;
        if (ESTestCase.randomBoolean()) {
            fields = new String[ESTestCase.randomIntBetween(1, 5)];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = ESTestCase.randomAlphaOfLengthBetween(3, 10);
            }
            getFieldMappingsRequest.fields(fields);
        } else if (ESTestCase.randomBoolean()) {
            getFieldMappingsRequest.fields((String[]) null);
        }

        Map<String, String> expectedParams = new HashMap<>();

        RequestConvertersTests.setRandomIndicesOptions(getFieldMappingsRequest::indicesOptions, getFieldMappingsRequest::indicesOptions,
            expectedParams);
        RequestConvertersTests.setRandomLocal(getFieldMappingsRequest::local, expectedParams);

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
        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));

        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertThat(HttpGet.METHOD_NAME, equalTo(request.getMethod()));
    }

    public void testDeleteIndex() {
        String[] indices = RequestConvertersTests.randomIndicesNames(0, 5);
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(deleteIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(deleteIndexRequest, expectedParams);

        RequestConvertersTests.setRandomIndicesOptions(deleteIndexRequest::indicesOptions, deleteIndexRequest::indicesOptions,
            expectedParams);

        Request request = IndicesRequestConverters.deleteIndex(deleteIndexRequest);
        Assert.assertEquals("/" + String.join(",", indices), request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        Assert.assertNull(request.getEntity());
    }

    public void testGetSettings() throws IOException {
        String[] indicesUnderTest = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(indicesUnderTest);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(getSettingsRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(getSettingsRequest::indicesOptions, getSettingsRequest::indicesOptions,
            expectedParams);

        RequestConvertersTests.setRandomLocal(getSettingsRequest, expectedParams);

        if (ESTestCase.randomBoolean()) {
            // the request object will not have include_defaults present unless it is set to
            // true
            getSettingsRequest.includeDefaults(ESTestCase.randomBoolean());
            if (getSettingsRequest.includeDefaults()) {
                expectedParams.put("include_defaults", Boolean.toString(true));
            }
        }

        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indicesUnderTest != null && indicesUnderTest.length > 0) {
            endpoint.add(String.join(",", indicesUnderTest));
        }
        endpoint.add("_settings");

        if (ESTestCase.randomBoolean()) {
            String[] names = ESTestCase.randomBoolean() ? null : new String[ESTestCase.randomIntBetween(0, 3)];
            if (names != null) {
                for (int x = 0; x < names.length; x++) {
                    names[x] = ESTestCase.randomAlphaOfLengthBetween(3, 10);
                }
            }
            getSettingsRequest.names(names);
            if (names != null && names.length > 0) {
                endpoint.add(String.join(",", names));
            }
        }

        Request request = IndicesRequestConverters.getSettings(getSettingsRequest);

        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        Assert.assertThat(request.getEntity(), nullValue());
    }

    public void testGetIndex() throws IOException {
        String[] indicesUnderTest = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);

        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(indicesUnderTest);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(getIndexRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(getIndexRequest::indicesOptions, getIndexRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomLocal(getIndexRequest, expectedParams);
        RequestConvertersTests.setRandomHumanReadable(getIndexRequest, expectedParams);

        if (ESTestCase.randomBoolean()) {
            // the request object will not have include_defaults present unless it is set to
            // true
            getIndexRequest.includeDefaults(ESTestCase.randomBoolean());
            if (getIndexRequest.includeDefaults()) {
                expectedParams.put("include_defaults", Boolean.toString(true));
            }
        }

        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indicesUnderTest != null && indicesUnderTest.length > 0) {
            endpoint.add(String.join(",", indicesUnderTest));
        }

        Request request = IndicesRequestConverters.getIndex(getIndexRequest);

        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        Assert.assertThat(request.getEntity(), nullValue());
    }

    public void testDeleteIndexEmptyIndices() {
        String[] indices = ESTestCase.randomBoolean() ? null : Strings.EMPTY_ARRAY;
        ActionRequestValidationException validationException = new DeleteIndexRequest(indices).validate();
        Assert.assertNotNull(validationException);
    }

    public void testOpenIndex() {
        String[] indices = RequestConvertersTests.randomIndicesNames(1, 5);
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(indices);
        openIndexRequest.indices(indices);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(openIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(openIndexRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(openIndexRequest::indicesOptions, openIndexRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomWaitForActiveShards(openIndexRequest::waitForActiveShards, expectedParams);

        Request request = IndicesRequestConverters.openIndex(openIndexRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "").add(String.join(",", indices)).add("_open");
        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
        Assert.assertThat(request.getEntity(), nullValue());
    }

    public void testOpenIndexEmptyIndices() {
        String[] indices = ESTestCase.randomBoolean() ? null : Strings.EMPTY_ARRAY;
        ActionRequestValidationException validationException = new OpenIndexRequest(indices).validate();
        Assert.assertNotNull(validationException);
    }

    public void testCloseIndex() {
        String[] indices = RequestConvertersTests.randomIndicesNames(1, 5);
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(closeIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(closeIndexRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(closeIndexRequest::indicesOptions, closeIndexRequest::indicesOptions,
            expectedParams);

        Request request = IndicesRequestConverters.closeIndex(closeIndexRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "").add(String.join(",", indices)).add("_close");
        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
        Assert.assertThat(request.getEntity(), nullValue());
    }

    public void testCloseIndexEmptyIndices() {
        String[] indices = ESTestCase.randomBoolean() ? null : Strings.EMPTY_ARRAY;
        ActionRequestValidationException validationException = new CloseIndexRequest(indices).validate();
        Assert.assertNotNull(validationException);
    }

    public void testRefresh() {
        String[] indices = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        RefreshRequest refreshRequest;
        if (ESTestCase.randomBoolean()) {
            refreshRequest = new RefreshRequest(indices);
        } else {
            refreshRequest = new RefreshRequest();
            refreshRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(refreshRequest::indicesOptions, refreshRequest::indicesOptions, expectedParams);
        Request request = IndicesRequestConverters.refresh(refreshRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_refresh");
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testFlush() {
        String[] indices = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        FlushRequest flushRequest;
        if (ESTestCase.randomBoolean()) {
            flushRequest = new FlushRequest(indices);
        } else {
            flushRequest = new FlushRequest();
            flushRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(flushRequest::indicesOptions, flushRequest::indicesOptions, expectedParams);
        if (ESTestCase.randomBoolean()) {
            flushRequest.force(ESTestCase.randomBoolean());
        }
        expectedParams.put("force", Boolean.toString(flushRequest.force()));
        if (ESTestCase.randomBoolean()) {
            flushRequest.waitIfOngoing(ESTestCase.randomBoolean());
        }
        expectedParams.put("wait_if_ongoing", Boolean.toString(flushRequest.waitIfOngoing()));

        Request request = IndicesRequestConverters.flush(flushRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_flush");
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testSyncedFlush() {
        String[] indices = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        SyncedFlushRequest syncedFlushRequest;
        if (ESTestCase.randomBoolean()) {
            syncedFlushRequest = new SyncedFlushRequest(indices);
        } else {
            syncedFlushRequest = new SyncedFlushRequest();
            syncedFlushRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(syncedFlushRequest::indicesOptions, syncedFlushRequest::indicesOptions,
            expectedParams);
        Request request = IndicesRequestConverters.flushSynced(syncedFlushRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
                endpoint.add(String.join(",", indices));
            }
        endpoint.add("_flush/synced");
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testForceMerge() {
        String[] indices = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        ForceMergeRequest forceMergeRequest;
        if (ESTestCase.randomBoolean()) {
            forceMergeRequest = new ForceMergeRequest(indices);
        } else {
            forceMergeRequest = new ForceMergeRequest();
            forceMergeRequest.indices(indices);
        }

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(forceMergeRequest::indicesOptions, forceMergeRequest::indicesOptions,
            expectedParams);
        if (ESTestCase.randomBoolean()) {
            forceMergeRequest.maxNumSegments(ESTestCase.randomInt());
        }
        expectedParams.put("max_num_segments", Integer.toString(forceMergeRequest.maxNumSegments()));
        if (ESTestCase.randomBoolean()) {
            forceMergeRequest.onlyExpungeDeletes(ESTestCase.randomBoolean());
        }
        expectedParams.put("only_expunge_deletes", Boolean.toString(forceMergeRequest.onlyExpungeDeletes()));
        if (ESTestCase.randomBoolean()) {
            forceMergeRequest.flush(ESTestCase.randomBoolean());
        }
        expectedParams.put("flush", Boolean.toString(forceMergeRequest.flush()));

        Request request = IndicesRequestConverters.forceMerge(forceMergeRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_forcemerge");
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testClearCache() {
        String[] indices = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        ClearIndicesCacheRequest clearIndicesCacheRequest;
        if (ESTestCase.randomBoolean()) {
            clearIndicesCacheRequest = new ClearIndicesCacheRequest(indices);
        } else {
            clearIndicesCacheRequest = new ClearIndicesCacheRequest();
            clearIndicesCacheRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(clearIndicesCacheRequest::indicesOptions, clearIndicesCacheRequest::indicesOptions,
            expectedParams);
        if (ESTestCase.randomBoolean()) {
            clearIndicesCacheRequest.queryCache(ESTestCase.randomBoolean());
        }
        expectedParams.put("query", Boolean.toString(clearIndicesCacheRequest.queryCache()));
        if (ESTestCase.randomBoolean()) {
            clearIndicesCacheRequest.fieldDataCache(ESTestCase.randomBoolean());
        }
        expectedParams.put("fielddata", Boolean.toString(clearIndicesCacheRequest.fieldDataCache()));
        if (ESTestCase.randomBoolean()) {
            clearIndicesCacheRequest.requestCache(ESTestCase.randomBoolean());
        }
        expectedParams.put("request", Boolean.toString(clearIndicesCacheRequest.requestCache()));
        if (ESTestCase.randomBoolean()) {
            clearIndicesCacheRequest.fields(RequestConvertersTests.randomIndicesNames(1, 5));
            expectedParams.put("fields", String.join(",", clearIndicesCacheRequest.fields()));
        }

        Request request = IndicesRequestConverters.clearCache(clearIndicesCacheRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_cache/clear");
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testExistsAlias() {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
        String[] indices = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        getAliasesRequest.indices(indices);
        // the HEAD endpoint requires at least an alias or an index
        boolean hasIndices = indices != null && indices.length > 0;
        String[] aliases;
        if (hasIndices) {
            aliases = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        } else {
            aliases = RequestConvertersTests.randomIndicesNames(1, 5);
        }
        getAliasesRequest.aliases(aliases);
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomLocal(getAliasesRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(getAliasesRequest::indicesOptions, getAliasesRequest::indicesOptions,
            expectedParams);

        Request request = IndicesRequestConverters.existsAlias(getAliasesRequest);
        StringJoiner expectedEndpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            expectedEndpoint.add(String.join(",", indices));
        }
        expectedEndpoint.add("_alias");
        if (aliases != null && aliases.length > 0) {
            expectedEndpoint.add(String.join(",", aliases));
        }
        Assert.assertEquals(HttpHead.METHOD_NAME, request.getMethod());
        Assert.assertEquals(expectedEndpoint.toString(), request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertNull(request.getEntity());
    }

    public void testExistsAliasNoAliasNoIndex() {
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
            IllegalArgumentException iae = LuceneTestCase.expectThrows(IllegalArgumentException.class,
                    () -> IndicesRequestConverters.existsAlias(getAliasesRequest));
            Assert.assertEquals("existsAlias requires at least an alias or an index", iae.getMessage());
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest((String[]) null);
            getAliasesRequest.indices((String[]) null);
            IllegalArgumentException iae = LuceneTestCase.expectThrows(IllegalArgumentException.class,
                    () -> IndicesRequestConverters.existsAlias(getAliasesRequest));
            Assert.assertEquals("existsAlias requires at least an alias or an index", iae.getMessage());
        }
    }

    public void testSplit() throws IOException {
        resizeTest(ResizeType.SPLIT, IndicesRequestConverters::split);
    }

    public void testSplitWrongResizeType() {
        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SHRINK);
        IllegalArgumentException iae = LuceneTestCase.expectThrows(IllegalArgumentException.class, ()
            -> IndicesRequestConverters.split(resizeRequest));
        Assert.assertEquals("Wrong resize type [SHRINK] for indices split request", iae.getMessage());
    }

    public void testShrinkWrongResizeType() {
        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        IllegalArgumentException iae = LuceneTestCase.expectThrows(IllegalArgumentException.class, ()
            -> IndicesRequestConverters.shrink(resizeRequest));
        Assert.assertEquals("Wrong resize type [SPLIT] for indices shrink request", iae.getMessage());
    }

    public void testShrink() throws IOException {
        resizeTest(ResizeType.SHRINK, IndicesRequestConverters::shrink);
    }

    private void resizeTest(ResizeType resizeType, CheckedFunction<ResizeRequest, Request, IOException> function)
            throws IOException {
        String[] indices = RequestConvertersTests.randomIndicesNames(2, 2);
        ResizeRequest resizeRequest = new ResizeRequest(indices[0], indices[1]);
        resizeRequest.setResizeType(resizeType);
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(resizeRequest, expectedParams);
        RequestConvertersTests.setRandomTimeout(resizeRequest::timeout, resizeRequest.timeout(), expectedParams);

        if (ESTestCase.randomBoolean()) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(ESTestCase.randomAlphaOfLengthBetween(3, 10));
            if (ESTestCase.randomBoolean()) {
                createIndexRequest.settings(randomIndexSettings());
            }
            if (ESTestCase.randomBoolean()) {
                randomAliases(createIndexRequest);
            }
            resizeRequest.setTargetIndex(createIndexRequest);
        }
        RequestConvertersTests.setRandomWaitForActiveShards(resizeRequest::setWaitForActiveShards, expectedParams);

        Request request = function.apply(resizeRequest);
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        String expectedEndpoint = "/" + resizeRequest.getSourceIndex() + "/_" + resizeType.name().toLowerCase(Locale.ROOT) + "/"
                + resizeRequest.getTargetIndexRequest().index();
        Assert.assertEquals(expectedEndpoint, request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        RequestConvertersTests.assertToXContentBody(resizeRequest, request.getEntity());
    }

    public void testRollover() throws IOException {
        RolloverRequest rolloverRequest = new RolloverRequest(ESTestCase.randomAlphaOfLengthBetween(3, 10),
                ESTestCase.randomBoolean() ? null : ESTestCase.randomAlphaOfLengthBetween(3, 10));
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(rolloverRequest::timeout, rolloverRequest.timeout(), expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(rolloverRequest, expectedParams);
        if (ESTestCase.randomBoolean()) {
            rolloverRequest.dryRun(ESTestCase.randomBoolean());
            if (rolloverRequest.isDryRun()) {
                expectedParams.put("dry_run", "true");
            }
        }
        if (ESTestCase.randomBoolean()) {
            rolloverRequest.addMaxIndexAgeCondition(new TimeValue(ESTestCase.randomNonNegativeLong()));
        }
        if (ESTestCase.randomBoolean()) {
            String type = ESTestCase.randomAlphaOfLengthBetween(3, 10);
            rolloverRequest.getCreateIndexRequest().mapping(type, RandomCreateIndexGenerator.randomMapping(type));
        }
        if (ESTestCase.randomBoolean()) {
            RandomCreateIndexGenerator.randomAliases(rolloverRequest.getCreateIndexRequest());
        }
        if (ESTestCase.randomBoolean()) {
            rolloverRequest.getCreateIndexRequest().settings(RandomCreateIndexGenerator.randomIndexSettings());
        }
        RequestConvertersTests.setRandomWaitForActiveShards(rolloverRequest.getCreateIndexRequest()::waitForActiveShards, expectedParams);

        Request request = IndicesRequestConverters.rollover(rolloverRequest);
        if (rolloverRequest.getNewIndexName() == null) {
            Assert.assertEquals("/" + rolloverRequest.getAlias() + "/_rollover", request.getEndpoint());
        } else {
            Assert.assertEquals("/" + rolloverRequest.getAlias() + "/_rollover/" + rolloverRequest.getNewIndexName(),
                request.getEndpoint());
        }
        Assert.assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(rolloverRequest, request.getEntity());
        Assert.assertEquals(expectedParams, request.getParameters());
    }

    public void testGetAlias() {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest();

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomLocal(getAliasesRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(getAliasesRequest::indicesOptions, getAliasesRequest::indicesOptions,
            expectedParams);

        String[] indices = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 2);
        String[] aliases = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 2);
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

        Assert.assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        Assert.assertEquals(expectedEndpoint.toString(), request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertNull(request.getEntity());
    }

    public void testIndexPutSettings() throws IOException {
        String[] indices = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 2);
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indices);
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(updateSettingsRequest, expectedParams);
        RequestConvertersTests.setRandomTimeout(updateSettingsRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(updateSettingsRequest::indicesOptions, updateSettingsRequest::indicesOptions,
            expectedParams);
        if (ESTestCase.randomBoolean()) {
            updateSettingsRequest.setPreserveExisting(ESTestCase.randomBoolean());
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
        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(updateSettingsRequest, request.getEntity());
        Assert.assertEquals(expectedParams, request.getParameters());
    }

    public void testPutTemplateRequest() throws Exception {
        Map<String, String> names = new HashMap<>();
        names.put("log", "log");
        names.put("template#1", "template%231");
        names.put("-#template", "-%23template");
        names.put("foo^bar", "foo%5Ebar");

        PutIndexTemplateRequest putTemplateRequest = new PutIndexTemplateRequest().name(ESTestCase.randomFrom(names.keySet()))
                .patterns(Arrays.asList(ESTestCase.generateRandomStringArray(20, 100, false, false)));
        if (ESTestCase.randomBoolean()) {
            putTemplateRequest.order(ESTestCase.randomInt());
        }
        if (ESTestCase.randomBoolean()) {
            putTemplateRequest.version(ESTestCase.randomInt());
        }
        if (ESTestCase.randomBoolean()) {
            putTemplateRequest.settings(Settings.builder().put("setting-" + ESTestCase.randomInt(), ESTestCase.randomTimeValue()));
        }
        if (ESTestCase.randomBoolean()) {
            putTemplateRequest.mapping("doc-" + ESTestCase.randomInt(),
                "field-" + ESTestCase.randomInt(), "type=" + ESTestCase.randomFrom("text", "keyword"));
        }
        if (ESTestCase.randomBoolean()) {
            putTemplateRequest.alias(new Alias("alias-" + ESTestCase.randomInt()));
        }
        Map<String, String> expectedParams = new HashMap<>();
        if (ESTestCase.randomBoolean()) {
            expectedParams.put("create", Boolean.TRUE.toString());
            putTemplateRequest.create(true);
        }
        if (ESTestCase.randomBoolean()) {
            String cause = ESTestCase.randomUnicodeOfCodepointLengthBetween(1, 50);
            putTemplateRequest.cause(cause);
            expectedParams.put("cause", cause);
        }
        RequestConvertersTests.setRandomMasterTimeout(putTemplateRequest, expectedParams);
        Request request = IndicesRequestConverters.putTemplate(putTemplateRequest);
        Assert.assertThat(request.getEndpoint(), equalTo("/_template/" + names.get(putTemplateRequest.name())));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        RequestConvertersTests.assertToXContentBody(putTemplateRequest, request.getEntity());
    }

    public void testValidateQuery() throws Exception {
        String[] indices = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        String[] types = ESTestCase.randomBoolean() ? ESTestCase.generateRandomStringArray(5, 5, false, false) : null;
        ValidateQueryRequest validateQueryRequest;
        if (ESTestCase.randomBoolean()) {
            validateQueryRequest = new ValidateQueryRequest(indices);
        } else {
            validateQueryRequest = new ValidateQueryRequest();
            validateQueryRequest.indices(indices);
        }
        validateQueryRequest.types(types);
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(validateQueryRequest::indicesOptions, validateQueryRequest::indicesOptions,
            expectedParams);
        validateQueryRequest.explain(ESTestCase.randomBoolean());
        validateQueryRequest.rewrite(ESTestCase.randomBoolean());
        validateQueryRequest.allShards(ESTestCase.randomBoolean());
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
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        RequestConvertersTests.assertToXContentBody(validateQueryRequest, request.getEntity());
        Assert.assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
    }

    public void testGetTemplateRequest() throws Exception {
        Map<String, String> encodes = new HashMap<>();
        encodes.put("log", "log");
        encodes.put("1", "1");
        encodes.put("template#1", "template%231");
        encodes.put("template-*", "template-*");
        encodes.put("foo^bar", "foo%5Ebar");
        List<String> names = ESTestCase.randomSubsetOf(1, encodes.keySet());
        GetIndexTemplatesRequest getTemplatesRequest = new GetIndexTemplatesRequest().names(names.toArray(new String[0]));
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(getTemplatesRequest, expectedParams);
        RequestConvertersTests.setRandomLocal(getTemplatesRequest, expectedParams);
        Request request = IndicesRequestConverters.getTemplates(getTemplatesRequest);
        Assert.assertThat(request.getEndpoint(),
            equalTo("/_template/" + names.stream().map(encodes::get).collect(Collectors.joining(","))));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
    }

    public void testDeleteTemplateRequest() {
        Map<String, String> encodes = new HashMap<>();
        encodes.put("log", "log");
        encodes.put("1", "1");
        encodes.put("template#1", "template%231");
        encodes.put("template-*", "template-*");
        encodes.put("foo^bar", "foo%5Ebar");
        DeleteIndexTemplateRequest deleteTemplateRequest = new DeleteIndexTemplateRequest().name(randomFrom(encodes.keySet()));
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(deleteTemplateRequest, expectedParams);
        Request request = IndicesRequestConverters.deleteTemplate(deleteTemplateRequest);
        Assert.assertThat(request.getMethod(), equalTo(HttpDelete.METHOD_NAME));
        Assert.assertThat(request.getEndpoint(), equalTo("/_template/" + encodes.get(deleteTemplateRequest.name())));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
    }
}
