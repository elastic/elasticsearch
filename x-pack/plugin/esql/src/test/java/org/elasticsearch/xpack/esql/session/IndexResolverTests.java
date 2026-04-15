/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class IndexResolverTests extends ESTestCase {

    public void testFieldCapsIndexNotFoundExceptionBecomesInvalidResolution() {
        Client client = mock(Client.class);
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<EsqlResolveFieldsResponse> listener = (ActionListener<EsqlResolveFieldsResponse>) invocation.getArguments()[2];
            listener.onFailure(new IndexNotFoundException("my-missing-index"));
            return null;
        }).when(client).execute(eq(EsqlResolveFieldsAction.TYPE), any(), any());

        IndexResolver resolver = new IndexResolver(client);
        AtomicReference<IndexResolution> resultRef = new AtomicReference<>();
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        resolver.resolveLookupIndices("my-missing-index", Set.of("*"), TransportVersion.current(), new ActionListener<>() {
            @Override
            public void onResponse(IndexResolution indexResolution) {
                resultRef.set(indexResolution);
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
            }
        });

        assertThat(failureRef.get(), nullValue());
        IndexResolution result = resultRef.get();
        assertThat(result, notNullValue());
        assertThat(result.isValid(), is(false));
        assertThat(result.toString(), containsString("Unknown index [my-missing-index]"));
    }

    public void testFieldCapsWrappedIndexNotFoundExceptionBecomesInvalidResolution() {
        Client client = mock(Client.class);
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<EsqlResolveFieldsResponse> listener = (ActionListener<EsqlResolveFieldsResponse>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("wrapper", new IndexNotFoundException("wrapped-index")));
            return null;
        }).when(client).execute(eq(EsqlResolveFieldsAction.TYPE), any(), any());

        IndexResolver resolver = new IndexResolver(client);
        AtomicReference<IndexResolution> resultRef = new AtomicReference<>();
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        resolver.resolveLookupIndices("wrapped-index", Set.of("*"), TransportVersion.current(), new ActionListener<>() {
            @Override
            public void onResponse(IndexResolution indexResolution) {
                resultRef.set(indexResolution);
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
            }
        });

        assertThat(failureRef.get(), nullValue());
        IndexResolution result = resultRef.get();
        assertThat(result, notNullValue());
        assertThat(result.isValid(), is(false));
        assertThat(result.toString(), containsString("Unknown index [wrapped-index]"));
    }

    @SuppressWarnings("unchecked")
    public void testFieldCapsOtherExceptionPropagatesAsFailure() {
        Client client = mock(Client.class);
        RuntimeException otherError = new RuntimeException("something else went wrong");
        doAnswer((Answer<Void>) invocation -> {
            ActionListener<EsqlResolveFieldsResponse> listener = (ActionListener<EsqlResolveFieldsResponse>) invocation.getArguments()[2];
            listener.onFailure(otherError);
            return null;
        }).when(client).execute(eq(EsqlResolveFieldsAction.TYPE), any(), any());

        IndexResolver resolver = new IndexResolver(client);
        AtomicReference<IndexResolution> resultRef = new AtomicReference<>();
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        resolver.resolveLookupIndices("some-index", Set.of("*"), TransportVersion.current(), new ActionListener<>() {
            @Override
            public void onResponse(IndexResolution indexResolution) {
                resultRef.set(indexResolution);
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
            }
        });

        assertThat(resultRef.get(), nullValue());
        assertThat(failureRef.get(), is(otherError));
    }

    public void testFieldCapsSuccessWithEmptyResponseProducesNotFoundForLookup() {
        Client client = mock(Client.class);
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<EsqlResolveFieldsResponse> listener = (ActionListener<EsqlResolveFieldsResponse>) invocation.getArguments()[2];
            FieldCapabilitiesResponse caps = new FieldCapabilitiesResponse(List.of(), List.of());
            listener.onResponse(new EsqlResolveFieldsResponse(caps));
            return null;
        }).when(client).execute(eq(EsqlResolveFieldsAction.TYPE), any(), any());

        IndexResolver resolver = new IndexResolver(client);
        AtomicReference<IndexResolution> resultRef = new AtomicReference<>();
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        resolver.resolveLookupIndices("missing-lookup", Set.of("*"), TransportVersion.current(), new ActionListener<>() {
            @Override
            public void onResponse(IndexResolution indexResolution) {
                resultRef.set(indexResolution);
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
            }
        });

        assertThat(failureRef.get(), nullValue());
        IndexResolution result = resultRef.get();
        assertThat(result, notNullValue());
        assertThat(result.isValid(), is(false));
        assertThat(result.toString(), containsString("Unknown index [missing-lookup]"));
    }
}
