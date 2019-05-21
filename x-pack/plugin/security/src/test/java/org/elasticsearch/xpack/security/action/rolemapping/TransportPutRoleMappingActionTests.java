/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.rolemapping;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingResponse;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TransportPutRoleMappingActionTests extends ESTestCase {

    private NativeRoleMappingStore store;
    private TransportPutRoleMappingAction action;
    private AtomicReference<PutRoleMappingRequest> requestRef;

    @Before
    public void setupMocks() {
        store = mock(NativeRoleMappingStore.class);
        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        action = new TransportPutRoleMappingAction(mock(ActionFilters.class), transportService, store);

        requestRef = new AtomicReference<>(null);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            requestRef.set((PutRoleMappingRequest) args[0]);
            ActionListener<Boolean> listener = (ActionListener) args[1];
            listener.onResponse(true);
            return null;
        }).when(store).putRoleMapping(any(PutRoleMappingRequest.class), any(ActionListener.class)
        );
    }

    public void testPutValidMapping() throws Exception {
        final FieldExpression expression = new FieldExpression(
                "username",
                Collections.singletonList(new FieldExpression.FieldValue("*"))
        );
        final PutRoleMappingResponse response = put("anarchy", expression, "superuser",
                Collections.singletonMap("dumb", true));

        assertThat(response.isCreated(), equalTo(true));

        final ExpressionRoleMapping mapping = requestRef.get().getMapping();
        assertThat(mapping.getExpression(), is(expression));
        assertThat(mapping.isEnabled(), equalTo(true));
        assertThat(mapping.getName(), equalTo("anarchy"));
        assertThat(mapping.getRoles(), iterableWithSize(1));
        assertThat(mapping.getRoles(), contains("superuser"));
        assertThat(mapping.getMetadata().size(), equalTo(1));
        assertThat(mapping.getMetadata().get("dumb"), equalTo(true));
    }

    private PutRoleMappingResponse put(String name, FieldExpression expression, String role,
                                       Map<String, Object> metadata) throws Exception {
        final PutRoleMappingRequest request = new PutRoleMappingRequest();
        request.setName(name);
        request.setRoles(Arrays.asList(role));
        request.setRules(expression);
        request.setMetadata(metadata);
        request.setEnabled(true);
        final PlainActionFuture<PutRoleMappingResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        return future.get();
    }
}
