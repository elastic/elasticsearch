/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp;

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.Set;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.DocumentVersion;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class ManageServiceProviderRestIT extends IdpRestTestCase {

    // From build.gradle
    private final String IDP_ENTITY_ID = "https://idp.test.es.elasticsearch.org/";
    // From SAMLConstants
    private final String REDIRECT_BINDING = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect";

    @Before
    public void defineApplicationPrivileges() throws IOException {
        super.createApplicationPrivileges(
            "elastic-cloud",
            org.elasticsearch.core.Map.of("deployment_admin", Set.of("sso:superuser"), "deployment_viewer", Set.of("sso:viewer"))
        );
    }

    public void testCreateAndDeleteServiceProvider() throws Exception {
        final String entityId = "ec:" + randomAlphaOfLength(8) + ":" + randomAlphaOfLength(12);
        final Map<String, Object> request = new HashMap<>();
        request.put("name", "Test SP");
        request.put("acs", "https://sp1.test.es.elasticsearch.org/saml/acs");
        final Map<String, Object> privilegeMap = new HashMap<>();
        privilegeMap.put("resource", entityId);
        privilegeMap.put("roles", Set.of("role:(\\w+)"));
        request.put("privileges", privilegeMap);
        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("principal", "https://idp.test.es.elasticsearch.org/attribute/principal");
        attributeMap.put("name", "https://idp.test.es.elasticsearch.org/attribute/name");
        attributeMap.put("email", "https://idp.test.es.elasticsearch.org/attribute/email");
        attributeMap.put("roles", "https://idp.test.es.elasticsearch.org/attribute/roles");
        request.put("attributes", attributeMap);
        final DocumentVersion docVersion = createServiceProvider(entityId, request);
        checkIndexDoc(docVersion);
        ensureGreen(SamlServiceProviderIndex.INDEX_NAME);
        getMetadata(entityId);
        deleteServiceProvider(entityId, docVersion);
        expectThrows(ResponseException.class, () -> getMetadata(entityId));
        expectThrows(ResponseException.class, () -> deleteServiceProvider(entityId, docVersion));
    }

    private void deleteServiceProvider(String entityId, DocumentVersion version) throws IOException {
        final Response response = client().performRequest(
            new Request("DELETE", "/_idp/saml/sp/" + encode(entityId) + "?refresh=" + RefreshPolicy.IMMEDIATE.getValue())
        );
        final Map<String, Object> map = entityAsMap(response);

        assertThat(ObjectPath.eval("document._id", map), equalTo(version.id));

        Long seqNo = asLong(ObjectPath.eval("document._seq_no", map));
        Long primaryTerm = asLong(ObjectPath.eval("document._primary_term", map));
        if (primaryTerm == version.primaryTerm) {
            assertThat(seqNo, greaterThanOrEqualTo(version.seqNo));
        } else {
            assertThat(primaryTerm, greaterThanOrEqualTo(version.primaryTerm));
        }

        assertThat(ObjectPath.eval("service_provider.entity_id", map), equalTo(entityId));
    }

    private void getMetadata(String entityId) throws IOException {
        final Map<String, Object> map = getAsMap("/_idp/saml/metadata/" + encode(entityId));
        assertThat(map, notNullValue());
        assertThat(map.keySet(), containsInAnyOrder("metadata"));
        final Object metadata = map.get("metadata");
        assertThat(metadata, notNullValue());
        assertThat(metadata, instanceOf(String.class));
        assertThat((String) metadata, containsString(IDP_ENTITY_ID));
        assertThat((String) metadata, containsString(REDIRECT_BINDING));
    }
}
