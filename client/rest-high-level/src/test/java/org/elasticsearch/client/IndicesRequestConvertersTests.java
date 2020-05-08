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
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.DeleteAliasRequest;
import org.elasticsearch.client.indices.GetFieldMappingsRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexTemplatesRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.IndexTemplatesExistRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.client.indices.RandomCreateIndexGenerator;
import org.elasticsearch.client.indices.ReloadAnalyzersRequest;
import org.elasticsearch.client.indices.ResizeRequest;
import org.elasticsearch.client.indices.rollover.RolloverRequest;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentType;
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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.client.indices.RandomCreateIndexGenerator.randomAliases;
import static org.elasticsearch.client.indices.RandomCreateIndexGenerator.randomMapping;
import static org.elasticsearch.index.RandomCreateIndexGenerator.randomAlias;
import static org.elasticsearch.index.RandomCreateIndexGenerator.randomIndexSettings;
import static org.elasticsearch.index.alias.RandomAliasActionsGenerator.randomAliasAction;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndicesRequestConvertersTests extends ESTestCase {

    public void testAnalyzeRequest() throws Exception {
        AnalyzeRequest indexAnalyzeRequest
            = AnalyzeRequest.withIndexAnalyzer("test_index", "test_analyzer", "Here is some text");

        Request request = IndicesRequestConverters.analyze(indexAnalyzeRequest);
        assertThat(request.getEndpoint(), equalTo("/test_index/_analyze"));
        RequestConvertersTests.assertToXContentBody(indexAnalyzeRequest, request.getEntity());

        AnalyzeRequest analyzeRequest = AnalyzeRequest.withGlobalAnalyzer("test_analyzer", "more text");
        assertThat(IndicesRequestConverters.analyze(analyzeRequest).getEndpoint(), equalTo("/_analyze"));
    }

    public void testIndicesExist() {
        String[] indices = RequestConvertersTests.randomIndicesNames(1, 10);

        GetIndexRequest getIndexRequest = new GetIndexRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(getIndexRequest::indicesOptions, getIndexRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomLocal(getIndexRequest::local, expectedParams);
        RequestConvertersTests.setRandomHumanReadable(getIndexRequest::humanReadable, expectedParams);
        RequestConvertersTests.setRandomIncludeDefaults(getIndexRequest::includeDefaults, expectedParams);

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
            -> IndicesRequestConverters.indicesExist(new GetIndexRequest((String[]) null)));
    }

    public void testCreateIndex() throws IOException {
        CreateIndexRequest createIndexRequest = RandomCreateIndexGenerator.randomCreateIndexRequest();

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(createIndexRequest, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(createIndexRequest, expectedParams);
        RequestConvertersTests.setRandomWaitForActiveShards(createIndexRequest::waitForActiveShards, expectedParams);

        Request request = IndicesRequestConverters.createIndex(createIndexRequest);
        Assert.assertEquals("/" + createIndexRequest.index(), request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(createIndexRequest, request.getEntity());
    }

    public void testCreateIndexNullIndex() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new CreateIndexRequest(null));
        assertEquals(e.getMessage(), "The index name cannot be null.");
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
        String[] indices = RequestConvertersTests.randomIndicesNames(0, 5);
        PutMappingRequest putMappingRequest = new PutMappingRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(putMappingRequest, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(putMappingRequest, expectedParams);

        Request request = IndicesRequestConverters.putMapping(putMappingRequest);

        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");

        Assert.assertEquals(endpoint.toString(), request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        RequestConvertersTests.assertToXContentBody(putMappingRequest, request.getEntity());
    }

    public void testGetMapping() {
        GetMappingsRequest getMappingRequest = new GetMappingsRequest();

        String[] indices = Strings.EMPTY_ARRAY;
        if (randomBoolean()) {
            indices = RequestConvertersTests.randomIndicesNames(0, 5);
            getMappingRequest.indices(indices);
        } else if (randomBoolean()) {
            getMappingRequest.indices((String[]) null);
        }

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(getMappingRequest::indicesOptions,
            getMappingRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomMasterTimeout(getMappingRequest, expectedParams);
        RequestConvertersTests.setRandomLocal(getMappingRequest::local, expectedParams);

        Request request = IndicesRequestConverters.getMappings(getMappingRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");

        Assert.assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        Assert.assertThat(expectedParams, equalTo(request.getParameters()));
        Assert.assertThat(HttpGet.METHOD_NAME, equalTo(request.getMethod()));
    }

    public void testGetFieldMapping() {
        GetFieldMappingsRequest getFieldMappingsRequest = new GetFieldMappingsRequest();

        String[] indices = Strings.EMPTY_ARRAY;
        if (randomBoolean()) {
            indices = RequestConvertersTests.randomIndicesNames(0, 5);
            getFieldMappingsRequest.indices(indices);
        } else if (randomBoolean()) {
            getFieldMappingsRequest.indices((String[]) null);
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
        RequestConvertersTests.setRandomIndicesOptions(getFieldMappingsRequest::indicesOptions, getFieldMappingsRequest::indicesOptions,
            expectedParams);

        Request request = IndicesRequestConverters.getFieldMapping(getFieldMappingsRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");
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

        RequestConvertersTests.setRandomLocal(getSettingsRequest::local, expectedParams);

        if (ESTestCase.randomBoolean()) {
            // the request object will not have include_defaults present unless it is set to
            // true
            getSettingsRequest.includeDefaults(ESTestCase.randomBoolean());
            if (getSettingsRequest.includeDefaults()) {
                expectedParams.put("include_defaults", Boolean.toString(true));
            }
        }

        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (CollectionUtils.isEmpty(indicesUnderTest) == false) {
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
            if (CollectionUtils.isEmpty(names) == false) {
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

        GetIndexRequest getIndexRequest = new GetIndexRequest(indicesUnderTest);

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(getIndexRequest, expectedParams);
        RequestConvertersTests.setRandomIndicesOptions(getIndexRequest::indicesOptions, getIndexRequest::indicesOptions, expectedParams);
        RequestConvertersTests.setRandomLocal(getIndexRequest::local, expectedParams);
        RequestConvertersTests.setRandomHumanReadable(getIndexRequest::humanReadable, expectedParams);

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
        RequestConvertersTests.setRandomTimeout(timeout -> closeIndexRequest.setTimeout(TimeValue.parseTimeValue(timeout, "test")),
            AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
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
        RequestConvertersTests.setRandomLocal(getAliasesRequest::local, expectedParams);
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

    public void testClone() throws IOException {
        resizeTest(ResizeType.CLONE, IndicesRequestConverters::clone);
    }

    public void testShrink() throws IOException {
        resizeTest(ResizeType.SHRINK, IndicesRequestConverters::shrink);
    }

    private void resizeTest(ResizeType resizeType, CheckedFunction<ResizeRequest, Request, IOException> function)
            throws IOException {
        String[] indices = RequestConvertersTests.randomIndicesNames(2, 2);
        ResizeRequest resizeRequest = new ResizeRequest(indices[0], indices[1]);
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(resizeRequest, expectedParams);
        RequestConvertersTests.setRandomTimeout(s -> resizeRequest.setTimeout(TimeValue.parseTimeValue(s, "timeout")),
            resizeRequest.timeout(), expectedParams);

        if (ESTestCase.randomBoolean()) {
            if (ESTestCase.randomBoolean()) {
                resizeRequest.setSettings(randomIndexSettings());
            }
            if (ESTestCase.randomBoolean()) {
                int count = randomIntBetween(0, 2);
                for (int i = 0; i < count; i++) {
                    resizeRequest.setAliases(List.of(randomAlias()));
                }
            }
        }
        RequestConvertersTests.setRandomWaitForActiveShards(resizeRequest::setWaitForActiveShards, expectedParams);
        if (resizeType == ResizeType.SPLIT) {
            resizeRequest.setSettings(Settings.builder().put("index.number_of_shards", 2).build());
        }

        Request request = function.apply(resizeRequest);
        Assert.assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        String expectedEndpoint = "/" + resizeRequest.getSourceIndex() + "/_" + resizeType.name().toLowerCase(Locale.ROOT) + "/"
                + resizeRequest.getTargetIndex();
        Assert.assertEquals(expectedEndpoint, request.getEndpoint());
        Assert.assertEquals(expectedParams, request.getParameters());
        RequestConvertersTests.assertToXContentBody(resizeRequest, request.getEntity());
    }

    public void testRollover() throws IOException {
        RolloverRequest rolloverRequest = new RolloverRequest(ESTestCase.randomAlphaOfLengthBetween(3, 10),
                ESTestCase.randomBoolean() ? null : ESTestCase.randomAlphaOfLengthBetween(3, 10));
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomTimeout(rolloverRequest, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
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
            rolloverRequest.getCreateIndexRequest().mapping(randomMapping());
        }
        if (ESTestCase.randomBoolean()) {
            randomAliases(rolloverRequest.getCreateIndexRequest());
        }
        if (ESTestCase.randomBoolean()) {
            rolloverRequest.getCreateIndexRequest().settings(
                org.elasticsearch.index.RandomCreateIndexGenerator.randomIndexSettings());
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
        RequestConvertersTests.setRandomLocal(getAliasesRequest::local, expectedParams);
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

        PutIndexTemplateRequest putTemplateRequest =
                new PutIndexTemplateRequest(ESTestCase.randomFrom(names.keySet()),
                    List.of(ESTestCase.generateRandomStringArray(20, 100, false, false)));
        if (ESTestCase.randomBoolean()) {
            putTemplateRequest.order(ESTestCase.randomInt());
        }
        if (ESTestCase.randomBoolean()) {
            putTemplateRequest.version(ESTestCase.randomInt());
        }
        if (ESTestCase.randomBoolean()) {
            putTemplateRequest.settings(Settings.builder().put("setting-" + ESTestCase.randomInt(), ESTestCase.randomTimeValue()));
        }
        Map<String, String> expectedParams = new HashMap<>();
        if (ESTestCase.randomBoolean()) {
            putTemplateRequest.mapping("{ \"properties\": { \"field-" + ESTestCase.randomInt() +
                    "\" : { \"type\" : \"" + ESTestCase.randomFrom("text", "keyword") + "\" }}}", XContentType.JSON);
        }
        if (ESTestCase.randomBoolean()) {
            putTemplateRequest.alias(new Alias("alias-" + ESTestCase.randomInt()));
        }
        if (ESTestCase.randomBoolean()) {
            expectedParams.put("create", Boolean.TRUE.toString());
            putTemplateRequest.create(true);
        }
        if (ESTestCase.randomBoolean()) {
            String cause = ESTestCase.randomUnicodeOfCodepointLengthBetween(1, 50);
            putTemplateRequest.cause(cause);
            expectedParams.put("cause", cause);
        }
        RequestConvertersTests.setRandomMasterTimeout(putTemplateRequest::masterNodeTimeout, expectedParams);

        Request request = IndicesRequestConverters.putTemplate(putTemplateRequest);
        Assert.assertThat(request.getEndpoint(), equalTo("/_template/" + names.get(putTemplateRequest.name())));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        RequestConvertersTests.assertToXContentBody(putTemplateRequest, request.getEntity());
    }
    public void testValidateQuery() throws Exception {
        String[] indices = ESTestCase.randomBoolean() ? null : RequestConvertersTests.randomIndicesNames(0, 5);
        ValidateQueryRequest validateQueryRequest;
        if (ESTestCase.randomBoolean()) {
            validateQueryRequest = new ValidateQueryRequest(indices);
        } else {
            validateQueryRequest = new ValidateQueryRequest();
            validateQueryRequest.indices(indices);
        }
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
        GetIndexTemplatesRequest getTemplatesRequest = new GetIndexTemplatesRequest(names);
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(getTemplatesRequest::setMasterNodeTimeout, expectedParams);
        RequestConvertersTests.setRandomLocal(getTemplatesRequest::setLocal, expectedParams);

        Request request = IndicesRequestConverters.getTemplates(getTemplatesRequest);
        Assert.assertThat(request.getEndpoint(),
            equalTo("/_template/" + names.stream().map(encodes::get).collect(Collectors.joining(","))));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());

        expectThrows(NullPointerException.class, () -> new GetIndexTemplatesRequest((String[]) null));
        expectThrows(NullPointerException.class, () -> new GetIndexTemplatesRequest((List<String>) null));
        expectThrows(IllegalArgumentException.class, () -> new GetIndexTemplatesRequest(singletonList(randomBoolean() ? "" : null)));
        expectThrows(IllegalArgumentException.class, () -> new GetIndexTemplatesRequest(new String[] { (randomBoolean() ? "" : null) }));
    }

    public void testTemplatesExistRequest() {
        final int numberOfNames = ESTestCase.usually()
            ? 1
            : ESTestCase.randomIntBetween(2, 20);
        final List<String> names = Arrays.asList(ESTestCase.randomArray(numberOfNames, numberOfNames, String[]::new,
            () -> ESTestCase.randomAlphaOfLengthBetween(1, 100)));
        final Map<String, String> expectedParams = new HashMap<>();
        final IndexTemplatesExistRequest indexTemplatesExistRequest = new IndexTemplatesExistRequest(names);
        RequestConvertersTests.setRandomMasterTimeout(indexTemplatesExistRequest::setMasterNodeTimeout, expectedParams);
        RequestConvertersTests.setRandomLocal(indexTemplatesExistRequest::setLocal, expectedParams);
        assertThat(indexTemplatesExistRequest.names(), equalTo(names));

        final Request request = IndicesRequestConverters.templatesExist(indexTemplatesExistRequest);
        assertThat(request.getMethod(), equalTo(HttpHead.METHOD_NAME));
        assertThat(request.getEndpoint(), equalTo("/_template/" + String.join(",", names)));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());

        expectThrows(NullPointerException.class, () -> new IndexTemplatesExistRequest((String[]) null));
        expectThrows(NullPointerException.class, () -> new IndexTemplatesExistRequest((List<String>) null));
        expectThrows(IllegalArgumentException.class, () -> new IndexTemplatesExistRequest(new String[] { (randomBoolean() ? "" : null) }));
        expectThrows(IllegalArgumentException.class, () -> new IndexTemplatesExistRequest(singletonList(randomBoolean() ? "" : null)));
        expectThrows(IllegalArgumentException.class, () -> new IndexTemplatesExistRequest(new String[] {}));
        expectThrows(IllegalArgumentException.class, () -> new IndexTemplatesExistRequest(emptyList()));
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

    public void testReloadAnalyzers() {
        String[] indices = RequestConvertersTests.randomIndicesNames(1, 5);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (CollectionUtils.isEmpty(indices) == false) {
            endpoint.add(String.join(",", indices));
        }
        ReloadAnalyzersRequest reloadRequest = new ReloadAnalyzersRequest(indices);
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(reloadRequest::setIndicesOptions, reloadRequest::indicesOptions,
                expectedParams);
        Request request = IndicesRequestConverters.reloadAnalyzers(reloadRequest);
        Assert.assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
        Assert.assertThat(request.getEndpoint(), equalTo(endpoint + "/_reload_search_analyzers"));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
    }

    public void testDeleteAlias() {
        DeleteAliasRequest deleteAliasRequest = new DeleteAliasRequest(randomAlphaOfLength(4), randomAlphaOfLength(4));

        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomMasterTimeout(deleteAliasRequest, expectedParams);
        RequestConvertersTests.setRandomTimeout(deleteAliasRequest, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);

        Request request = IndicesRequestConverters.deleteAlias(deleteAliasRequest);
        Assert.assertThat(request.getMethod(), equalTo(HttpDelete.METHOD_NAME));
        Assert.assertThat(request.getEndpoint(), equalTo("/" + deleteAliasRequest.getIndex() + "/_alias/" + deleteAliasRequest.getAlias()));
        Assert.assertThat(request.getParameters(), equalTo(expectedParams));
        Assert.assertThat(request.getEntity(), nullValue());
    }
}
