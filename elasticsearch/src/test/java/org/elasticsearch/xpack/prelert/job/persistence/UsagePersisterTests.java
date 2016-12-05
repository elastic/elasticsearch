/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.elasticsearch.xpack.prelert.job.usage.Usage;

public class UsagePersisterTests extends ESTestCase {
    @SuppressWarnings("rawtypes")
    public void testPersistUsageCounts() throws ParseException {
        Client client = mock(Client.class);
        final UpdateRequestBuilder updateRequestBuilder = createSelfReturningUpdateRequester();

        when(client.prepareUpdate(anyString(), anyString(), anyString())).thenReturn(
                updateRequestBuilder);

        UsagePersister persister = new UsagePersister(Settings.EMPTY, client);

        persister.persistUsage("job1", 10L, 30L, 1L);

        ArgumentCaptor<String> indexCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> upsertsCaptor = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Script> updateScriptCaptor = ArgumentCaptor.forClass(Script.class);

        verify(client, times(2)).prepareUpdate(indexCaptor.capture(), eq(Usage.TYPE),
                idCaptor.capture());
        verify(updateRequestBuilder, times(2)).setScript(updateScriptCaptor.capture());
        verify(updateRequestBuilder, times(2)).setUpsert(upsertsCaptor.capture());
        verify(updateRequestBuilder, times(2)).setRetryOnConflict(ElasticsearchScripts.UPDATE_JOB_RETRY_COUNT);
        verify(updateRequestBuilder, times(2)).get();

        assertEquals(Arrays.asList("prelert-usage", "prelertresults-job1"), indexCaptor.getAllValues());
        assertEquals(2, idCaptor.getAllValues().size());
        String id = idCaptor.getValue();
        assertEquals(id, idCaptor.getAllValues().get(0));
        assertTrue(id.startsWith("usage-"));
        String timestamp = id.substring("usage-".length());

        assertEquals("prelert-usage", indexCaptor.getAllValues().get(0));
        assertEquals("prelertresults-job1", indexCaptor.getAllValues().get(1));

        Script script = updateScriptCaptor.getValue();
        assertEquals("ctx._source.inputBytes += params.bytes;ctx._source.inputFieldCount += params.fieldCount;ctx._source.inputRecordCount"
                + " += params.recordCount;", script.getIdOrCode());
        assertEquals(ScriptType.INLINE, script.getType());
        assertEquals("painless", script.getLang());
        Map<String, Object> scriptParams = script.getParams();
        assertEquals(3, scriptParams.size());
        assertEquals(10L, scriptParams.get("bytes"));
        assertEquals(30L, scriptParams.get("fieldCount"));
        assertEquals(1L, scriptParams.get("recordCount"));

        List<Map> capturedUpserts = upsertsCaptor.getAllValues();
        assertEquals(2, capturedUpserts.size());
        assertEquals(timestamp, capturedUpserts.get(0).get(ElasticsearchMappings.ES_TIMESTAMP).toString());
        assertEquals(10L, capturedUpserts.get(0).get(Usage.INPUT_BYTES));
        assertEquals(30L, capturedUpserts.get(0).get(Usage.INPUT_FIELD_COUNT));
        assertEquals(1L, capturedUpserts.get(0).get(Usage.INPUT_RECORD_COUNT));
    }

    private UpdateRequestBuilder createSelfReturningUpdateRequester() {
        return mock(UpdateRequestBuilder.class, new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                if (invocation.getMethod().getReturnType() == UpdateRequestBuilder.class) {
                    return invocation.getMock();
                }
                return null;
            }
        });
    }
}
