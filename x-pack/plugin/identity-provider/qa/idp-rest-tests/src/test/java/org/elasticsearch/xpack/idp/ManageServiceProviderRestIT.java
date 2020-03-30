/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp;

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.DocumentVersion;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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

    public void testCreateAndDeleteServiceProvider() throws Exception {
        final String entityId = "ec:" + randomAlphaOfLength(8) + ":" + randomAlphaOfLength(12);
        final Map<String, Object> request = Map.ofEntries(
            Map.entry("name", "Test SP"),
            Map.entry("acs", "https://sp1.test.es.elasticsearch.org/saml/acs"),
            Map.entry("privileges", Map.ofEntries(
                Map.entry("resource", entityId),
                Map.entry("roles", Map.of("superuser", "role:superuser", "viewer", "role:viewer"))
            )),
            Map.entry("attributes", Map.ofEntries(
                Map.entry("principal", "https://idp.test.es.elasticsearch.org/attribute/principal"),
                Map.entry("name", "https://idp.test.es.elasticsearch.org/attribute/name"),
                Map.entry("email", "https://idp.test.es.elasticsearch.org/attribute/email"),
                Map.entry("roles", "https://idp.test.es.elasticsearch.org/attribute/roles")
            )));
        final DocumentVersion docVersion = createServiceProvider(entityId, request);
        checkIndexDoc(docVersion);
        ensureGreen(SamlServiceProviderIndex.INDEX_NAME);
        getMetaData(entityId);
        deleteServiceProvider(entityId, docVersion);
        expectThrows(ResponseException.class, () -> getMetaData(entityId));
        expectThrows(ResponseException.class, () -> deleteServiceProvider(entityId, docVersion));
    }

    private DocumentVersion createServiceProvider(String entityId, Map<String, Object> body) throws IOException {
        final Request request = new Request("PUT", "/_idp/saml/sp/" + encode(entityId) + "?refresh=" + RefreshPolicy.IMMEDIATE.getValue());
        final String entity = Strings.toString(JsonXContent.contentBuilder().map(body));
        request.setJsonEntity(entity);
        final Response response = client().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(ObjectPath.eval("service_provider.entity_id", map), equalTo(entityId));
        assertThat(ObjectPath.eval("service_provider.enabled", map), equalTo(true));

        final Object docId = ObjectPath.eval("document._id", map);
        final Object seqNo = ObjectPath.eval("document._seq_no", map);
        final Object primaryTerm = ObjectPath.eval("document._primary_term", map);
        assertThat(docId, instanceOf(String.class));
        assertThat(seqNo, instanceOf(Number.class));
        assertThat(primaryTerm, instanceOf(Number.class));
        return new DocumentVersion((String) docId, asLong(primaryTerm), asLong(seqNo));
    }

    private void checkIndexDoc(DocumentVersion docVersion) throws IOException {
        final Request request = new Request("GET", SamlServiceProviderIndex.INDEX_NAME + "/_doc/" + docVersion.id);
        final Response response = adminClient().performRequest(request);
        final Map<String, Object>
            map = entityAsMap(response);
        assertThat(map.get("_index"), equalTo(SamlServiceProviderIndex.INDEX_NAME));
        assertThat(map.get("_id"), equalTo(docVersion.id));
        assertThat(asLong(map.get("_seq_no")), equalTo(docVersion.seqNo));
        assertThat(asLong(map.get("_primary_term")), equalTo(docVersion.primaryTerm));
    }

    private void deleteServiceProvider(String entityId, DocumentVersion version) throws IOException {
        final Response response = client().performRequest(new Request("DELETE",
            "/_idp/saml/sp/" + encode(entityId) + "?refresh=" + RefreshPolicy.IMMEDIATE.getValue()));
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

    private void getMetaData(String entityId) throws IOException {
        final Map<String, Object> map = getAsMap("/_idp/saml/metadata/" + encode(entityId));
        assertThat(map, notNullValue());
        assertThat(map.keySet(), containsInAnyOrder("metadata"));
        final Object metadata = map.get("metadata");
        assertThat(metadata, notNullValue());
        assertThat(metadata, instanceOf(String.class));
        assertThat((String) metadata, containsString(IDP_ENTITY_ID));
        assertThat((String) metadata, containsString(REDIRECT_BINDING));
    }

    private String encode(String param) {
        return URLEncoder.encode(param, StandardCharsets.UTF_8);
    }

    private Long asLong(Object val) {
        if (val == null) {
            return null;
        }
        if (val instanceof Long) {
            return (Long) val;
        }
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        if (val instanceof String) {
            return Long.parseLong((String) val);
        }
        throw new IllegalArgumentException("Value [" + val + "] of type [" + val.getClass() + "] is not a Long");
    }
}
