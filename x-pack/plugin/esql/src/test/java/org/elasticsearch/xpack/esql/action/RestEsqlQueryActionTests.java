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
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.action.RestEsqlQueryAction.CLIENT_META;
import static org.elasticsearch.xpack.esql.action.RestEsqlQueryAction.PRODUCT_ORIGIN;
import static org.elasticsearch.xpack.esql.action.RestEsqlQueryAction.defaultVersionForOldClients;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RestEsqlQueryActionTests extends ESTestCase {
    public void testNoVersionForNoClient() {
        assertEsqlVersion(null, null, nullValue(String.class));
    }

    public void testNoVersionForAlreadySet() {
        EsqlQueryRequest esqlRequest = new EsqlQueryRequest();
        esqlRequest.esqlVersion("whatever");
        FakeRestRequest restRequest = new FakeRestRequest();
        Supplier<String> version = randomFrom(
            () -> "es=8.1" + between(0, 3), // Versions we would rewrite.
            () -> "es=8.1" + between(4, 9), // We wouldn't rewrite these anyway, but let's try it sometimes.
            () -> "es=8." + between(0, 9) + between(0, 9), // These will rarely spit out versions we would rewrite. Either is fine.
            () -> "es=" + between(0, 9) + "." + between(0, 9) + between(0, 9)
        );
        restRequest.getHttpRequest().getHeaders().put(CLIENT_META, List.of("es=8.13.0"));
        defaultVersionForOldClients(esqlRequest, restRequest);
        assertThat(esqlRequest.esqlVersion(), equalTo("whatever"));
    }

    public void testNoVersionForNewClient() {
        Supplier<String> version = randomFrom(
            () -> "es=8.14",
            () -> "es=8.2" + between(0, 9),
            () -> "es=8." + between(3, 9) + between(0, 9),
            () -> "es=9." + between(0, 9) + between(0, 9),
            () -> "es=" + between(0, 9) + between(0, 9) + "." + between(0, 9) + between(0, 9)
        );
        assertEsqlVersion(version.get(), randomProduct(), nullValue(String.class));
    }

    public void testAddsVersionForPython813() {
        assertAddsOldest(
            randomFrom(
                "es=8.13.0,py=3.11.8,t=8.13.0,ur=2.2.1", // This is what the python client sent for me on 2024-4-12
                "py=3.11.8,es=8.13.0,ur=2.2.1,t=8.13.0", // This is just a jumbled version of the above
                "es=8.13" // This is all we need to trigger it
            ),
            randomProduct()
        );
    }

    public void testAddsVersionForPython812() {
        assertAddsOldest(
            randomFrom(
                "es=8.12.0,py=3.11.8,t=8.13.0,ur=2.2.1", // This is what the python client sent for me on 2024-4-12
                "py=3.11.8,t=8.13.0,es=8.12.0,ur=2.2.1", // This is just a jumbled version of the above
                "es=8.12" // This is all we need to trigger it
            ),
            randomProduct()
        );
    }

    public void testNoVersionForKibana814() {
        assertEsqlVersion("es=8.13", "kibana", nullValue(String.class));
    }

    public void testAddsVersionForKibana813() {
        assertAddsOldest(
            randomFrom(
                "es=8.9.1p,js=20.12.2,t=8.3.3,hc=20.12.2", // This is what kibana sent on 2024-4-12
                "js=20.12.2,es=8.9.1p,t=8.3.3,hc=20.12.2", // This is just a jumbled version of the above
                "es=8.9" // This is all we need to trigger it
            ),
            "kibana"
        );
    }

    public void testAddsVersionForKibana812() {
        assertAddsOldest(
            randomFrom(
                "es=8.9.1p,js=18.19.1,t=8.3.3,hc=18.19.1", // This is what kibana sent on 2024-4-12
                "js=18.19.1,t=8.3.3,es=8.9.1p,hc=18.19.1", // This is just a jumbled version of the above
                "es=8.9" // This is all we need to trigger it
            ),
            "kibana"
        );
    }

    public void testAddsVersionForKibana811() {
        assertAddsOldest(
            randomFrom(
                "es=8.9.1p,js=18.18.2,t=8.3.3,hc=18.18.2", // This is what kibana sent on 2024-4-12
                "js=18.18.2,es=8.9.1p,t=8.3.3,hc=18.18.2", // This is just a jumbled version of the above
                "es=8.9" // This is all we need to trigger it
            ),
            "kibana"
        );
    }

    private void assertAddsOldest(String clientMeta, String elasticProductOrigin) {
        assertEsqlVersion(clientMeta, elasticProductOrigin, equalTo(EsqlVersion.ROCKET.versionStringWithoutEmoji()));
    }

    private void assertEsqlVersion(String clientMeta, String elasticProductOrigin, Matcher<String> expectedEsqlVersion) {
        EsqlQueryRequest esqlRequest = new EsqlQueryRequest();
        FakeRestRequest restRequest = new FakeRestRequest();
        if (clientMeta != null) {
            restRequest.getHttpRequest().getHeaders().put(CLIENT_META, List.of(clientMeta));
        }
        if (elasticProductOrigin != null) {
            restRequest.getHttpRequest().getHeaders().put(PRODUCT_ORIGIN, List.of(elasticProductOrigin));
        }
        defaultVersionForOldClients(esqlRequest, restRequest);
        assertThat(esqlRequest.esqlVersion(), expectedEsqlVersion);
    }

    /**
     * Returns {@code null} or a random string that <strong>isn't</strong> {@code kibana}.
     */
    private String randomProduct() {
        return randomBoolean() ? null : randomAlphaOfLength(3);
    }
}
