/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.esql.version.EsqlVersion;
import org.hamcrest.Matcher;

import java.util.List;

import static org.elasticsearch.xpack.esql.action.RestEsqlQueryAction.CLIENT_META;
import static org.elasticsearch.xpack.esql.action.RestEsqlQueryAction.defaultVersionForOldClients;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RestEsqlQueryActionTests extends ESTestCase {
    public void testNoVersionForNoClient() {
        EsqlQueryRequest esqlRequest = new EsqlQueryRequest();
        FakeRestRequest restRequest = new FakeRestRequest();
        defaultVersionForOldClients(esqlRequest, restRequest);
        assertThat(esqlRequest.esqlVersion(), nullValue());
    }

    public void testNoVersionForAlreadySet() {
        EsqlQueryRequest esqlRequest = new EsqlQueryRequest();
        esqlRequest.esqlVersion("whatever");
        FakeRestRequest restRequest = new FakeRestRequest();
        restRequest.getHttpRequest().getHeaders().put(CLIENT_META, List.of("es=8.13.0"));
        defaultVersionForOldClients(esqlRequest, restRequest);
        assertThat(esqlRequest.esqlVersion(), equalTo("whatever"));
    }

    public void testNoVersionForNewClient() {
        assertEsqlVersion("es=8.14.0", nullValue(String.class));
    }

    public void testAddsVersionForPython813() {
        assertAddsOldest(
            randomFrom(
                "es=8.13.0,py=3.11.8,t=8.13.0,ur=2.2.1", // This is what the python client sent for me on 2024-4-12
                "py=3.11.8,es=8.13.0,ur=2.2.1,t=8.13.0", // This is just a jumbled version of the above
                "es=8.13.0" // This is all we need to trigger it
            )
        );
    }

    public void testAddsVersionForPython812() {
        assertAddsOldest(
            randomFrom(
                "es=8.12.0,py=3.11.8,t=8.13.0,ur=2.2.1", // This is what the python client sent for me on 2024-4-12
                "py=3.11.8,t=8.13.0,es=8.12.0,ur=2.2.1", // This is just a jumbled version of the above
                "es=8.12.0" // This is all we need to trigger it
            )
        );
    }

    private void assertAddsOldest(String clientMeta) {
        assertEsqlVersion(clientMeta, equalTo(EsqlVersion.ROCKET.versionStringWithoutEmoji()));
    }

    private void assertEsqlVersion(String clientMeta, Matcher<String> expectedEsqlVersion) {
        EsqlQueryRequest esqlRequest = new EsqlQueryRequest();
        FakeRestRequest restRequest = new FakeRestRequest();
        restRequest.getHttpRequest().getHeaders().put(CLIENT_META, List.of(clientMeta));
        defaultVersionForOldClients(esqlRequest, restRequest);
        assertThat(esqlRequest.esqlVersion(), expectedEsqlVersion);
    }
}
