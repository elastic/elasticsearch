/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;

public class ElasticsearchScriptsTests extends ESTestCase {
    @Captor
    private ArgumentCaptor<Map<String, Object>> mapCaptor;

    @Before
    public void setUpMocks() throws InterruptedException, ExecutionException {
        MockitoAnnotations.initMocks(this);
    }

    public void testNewUpdateBucketCount() {
        Script script = ElasticsearchScripts.newUpdateBucketCount(42L);
        assertEquals("ctx._source.counts.bucketCount += params.count", script.getIdOrCode());
        assertEquals(1, script.getParams().size());
        assertEquals(42L, script.getParams().get("count"));
    }

    public void testNewUpdateUsage() {
        Script script = ElasticsearchScripts.newUpdateUsage(1L, 2L, 3L);
        assertEquals(
                "ctx._source.inputBytes += params.bytes;ctx._source.inputFieldCount += params.fieldCount;ctx._source.inputRecordCount"
                        + " += params.recordCount;",
                        script.getIdOrCode());
        assertEquals(3, script.getParams().size());
        assertEquals(1L, script.getParams().get("bytes"));
        assertEquals(2L, script.getParams().get("fieldCount"));
        assertEquals(3L, script.getParams().get("recordCount"));
    }

    public void testUpdateProcessingTime() {
        Long time = 135790L;
        Script script = ElasticsearchScripts.updateProcessingTime(time);
        assertEquals("ctx._source.averageProcessingTimeMs = ctx._source.averageProcessingTimeMs * 0.9 + params.timeMs * 0.1",
                script.getIdOrCode());
        assertEquals(time, script.getParams().get("timeMs"));
    }

    public void testUpdateUpsertViaScript() {
        String index = "idx";
        String docId = "docId";
        String type = "type";
        Map<String, Object> map = new HashMap<>();
        map.put("testKey", "testValue");

        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "test-script-here", map);
        ArgumentCaptor<Script> captor = ArgumentCaptor.forClass(Script.class);

        MockClientBuilder clientBuilder = new MockClientBuilder("cluster").prepareUpdateScript(index, type, docId, captor, mapCaptor);
        Client client = clientBuilder.build();

        assertTrue(ElasticsearchScripts.updateViaScript(client, index, type, docId, script));

        Script response = captor.getValue();
        assertEquals(script, response);
        assertEquals(map, response.getParams());

        map.clear();
        map.put("secondKey", "secondValue");
        map.put("thirdKey", "thirdValue");
        assertTrue(ElasticsearchScripts.upsertViaScript(client, index, type, docId, script, map));

        Map<String, Object> updatedParams = mapCaptor.getValue();
        assertEquals(map, updatedParams);
    }

    public void testUpdateUpsertViaScript_InvalidIndex() {
        String index = "idx";
        String docId = "docId";
        String type = "type";

        IndexNotFoundException e = new IndexNotFoundException("INF");

        Script script = new Script("foo");
        ArgumentCaptor<Script> captor = ArgumentCaptor.forClass(Script.class);

        MockClientBuilder clientBuilder = new MockClientBuilder("cluster").prepareUpdateScript(index, type, docId, captor, mapCaptor, e);
        Client client = clientBuilder.build();

        try {
            ElasticsearchScripts.updateViaScript(client, index, type, docId, script);
            assertFalse(true);
        } catch (ResourceNotFoundException ex) {
            assertThat(ex.getMessage(), containsString(index));
        }
    }

    public void testUpdateUpsertViaScript_IllegalArgument() {
        String index = "idx";
        String docId = "docId";
        String type = "type";
        Map<String, Object> map = new HashMap<>();
        map.put("testKey", "testValue");

        IllegalArgumentException ex = new IllegalArgumentException();

        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "test-script-here", map);
        ArgumentCaptor<Script> captor = ArgumentCaptor.forClass(Script.class);

        MockClientBuilder clientBuilder = new MockClientBuilder("cluster").prepareUpdateScript(index, type, docId, captor, mapCaptor, ex);
        Client client = clientBuilder.build();

        expectThrows(IllegalArgumentException.class, () -> ElasticsearchScripts.updateViaScript(client, index, type, docId, script));
    }

}
