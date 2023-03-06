/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.entsearch.EnterpriseSearchTransportAction;
import org.elasticsearch.xpack.entsearch.engine.EngineIndexService;
import org.elasticsearch.xpack.entsearch.engine.EngineTestUtils;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportListEnginesActionTests extends ESTestCase {
    public void testWithUnsupportedLicense() {
        MockLicenseState licenseState = mock(MockLicenseState.class);

        when(licenseState.isAllowed(EnterpriseSearchTransportAction.LICENSED_ENGINE_FEATURE)).thenReturn(false);
        when(licenseState.isActive()).thenReturn(false);
        when(licenseState.statusDescription()).thenReturn("invalid license");

        TransportListEnginesAction transportAction = new TransportListEnginesAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(EngineIndexService.class),
            licenseState
        );

        PageParams pageParams = EngineTestUtils.randomPageParams();
        String query = randomFrom(new String[] { null, randomAlphaOfLengthBetween(1, 10) });
        ListEnginesAction.Request request = new ListEnginesAction.Request(query, pageParams);

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ListEnginesAction.Response> responseRef = new AtomicReference<>();

        transportAction.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(ListEnginesAction.Response response) {
                responseRef.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(ElasticsearchSecurityException.class));
        assertThat(throwableRef.get().getMessage(), containsString("Engines require an active trial or platinum license"));
    }
}
