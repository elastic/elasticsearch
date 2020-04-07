/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.execution.PlanExecutor;
import org.elasticsearch.xpack.eql.plugin.TransportEqlSearchAction;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CancellationTests extends ESTestCase {

    public void testCancellationBeforeFieldCaps() throws InterruptedException {
        Client client = mock(Client.class);
        EqlSearchTask task = mock(EqlSearchTask.class);
        when(task.isCancelled()).thenReturn(true);

        IndexResolver indexResolver = new IndexResolver(client, randomAlphaOfLength(10), DefaultDataTypeRegistry.INSTANCE);
        PlanExecutor planExecutor = new PlanExecutor(client, indexResolver, new NamedWriteableRegistry(Collections.emptyList()));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        TransportEqlSearchAction.operation(planExecutor, task, new EqlSearchRequest().query("foo where blah"), "", "",
            new ActionListener<>() {
                @Override
                public void onResponse(EqlSearchResponse eqlSearchResponse) {
                    fail("Shouldn't be here");
                    countDownLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(TaskCancelledException.class));
                    countDownLatch.countDown();
                }
            });
        countDownLatch.await();
        verify(task, times(1)).isCancelled();
        verifyNoMoreInteractions(client, task);
    }

    public void testCancellationBeforeSearch() throws InterruptedException {
        Client client = mock(Client.class);

        AtomicBoolean cancelled = new AtomicBoolean(false);
        EqlSearchTask task = mock(EqlSearchTask.class);
        when(task.isCancelled()).then(invocationOnMock -> cancelled.get());

        String[] indices = new String[]{"endgame"};

        FieldCapabilities fooField =
            new FieldCapabilities("foo", "integer", true, true, indices, null, null, emptyMap());
        FieldCapabilities categoryField =
            new FieldCapabilities("event.category", "keyword", true, true, indices, null, null, emptyMap());
        FieldCapabilities timestampField =
            new FieldCapabilities("@timestamp", "date", true, true, indices, null, null, emptyMap());
        Map<String, Map<String, FieldCapabilities>> fields = new HashMap<>();
        fields.put(fooField.getName(), singletonMap(fooField.getName(), fooField));
        fields.put(categoryField.getName(), singletonMap(categoryField.getName(), categoryField));
        fields.put(timestampField.getName(), singletonMap(timestampField.getName(), timestampField));

        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getIndices()).thenReturn(indices);
        when(fieldCapabilitiesResponse.get()).thenReturn(fields);
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<FieldCapabilitiesResponse> listener = (ActionListener<FieldCapabilitiesResponse>) invocation.getArguments()[1];
            assertFalse(cancelled.getAndSet(true));
            listener.onResponse(fieldCapabilitiesResponse);
            return null;
        }).when(client).fieldCaps(any(), any());


        IndexResolver indexResolver = new IndexResolver(client, randomAlphaOfLength(10), DefaultDataTypeRegistry.INSTANCE);
        PlanExecutor planExecutor = new PlanExecutor(client, indexResolver, new NamedWriteableRegistry(Collections.emptyList()));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        TransportEqlSearchAction.operation(planExecutor, task, new EqlSearchRequest().indices("endgame")
            .query("process where foo==3"), "", "", new ActionListener<>() {
            @Override
            public void onResponse(EqlSearchResponse eqlSearchResponse) {
                fail("Shouldn't be here");
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(TaskCancelledException.class));
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
        verify(client).fieldCaps(any(), any());
        verify(task, times(2)).isCancelled();
        verifyNoMoreInteractions(client, task);
    }

}
