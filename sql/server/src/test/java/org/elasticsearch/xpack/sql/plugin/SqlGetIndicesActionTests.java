/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.catalog.EsIndex;
import org.elasticsearch.xpack.sql.analysis.catalog.IndexResolver;
import org.mockito.Matchers;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SqlGetIndicesActionTests extends ESTestCase {

    public void testOperation() throws IOException, InterruptedException {
        SqlGetIndicesAction.Request request = new SqlGetIndicesAction.Request(IndicesOptions.lenientExpandOpen(), "test", "bar", "foo*");

        List<EsIndex> esIndices = new ArrayList<>();
        esIndices.add(new EsIndex("foo1", Collections.emptyMap()));
        esIndices.add(new EsIndex("foo2", Collections.emptyMap()));
        esIndices.add(new EsIndex("test", Collections.emptyMap()));

        final AtomicReference<SqlGetIndicesAction.Response> responseRef = new AtomicReference<>();
        final AtomicReference<Exception> errorRef = new AtomicReference<>();

        ActionListener<SqlGetIndicesAction.Response> listener = new ActionListener<SqlGetIndicesAction.Response>() {
            @Override
            public void onResponse(SqlGetIndicesAction.Response response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                errorRef.set(e);
            }
        };

        IndexResolver indexResolver = mock(IndexResolver.class);
        doAnswer((Answer<Void>) invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<List<EsIndex>> actionListener = (ActionListener<List<EsIndex>>)invocationOnMock.getArguments()[0];
            actionListener.onResponse(esIndices);
            return null;
        }).when(indexResolver).asList(any(), Matchers.<String>anyVararg());

        SqlGetIndicesAction.operation(indexResolver, request, listener);

        assertNull(errorRef.get());
        assertNotNull(responseRef.get());
        SqlGetIndicesAction.Response response = responseRef.get();
        assertThat(response.indices(), hasSize(3));
        assertEquals("foo1", response.indices().get(0).name());
        assertEquals("foo2", response.indices().get(1).name());
        assertEquals("test", response.indices().get(2).name());
    }
}
