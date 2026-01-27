/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;
import org.junit.ClassRule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public abstract class IdpRestTestCase extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.idp.enabled", "true")
        .setting("xpack.idp.entity_id", "https://idp.test.es.elasticsearch.org/")
        .setting("xpack.idp.sso_endpoint.redirect", "http://idp.test.es.elasticsearch.org/test/saml/redirect")
        .setting("xpack.idp.signing.certificate", "idp-sign.crt")
        .setting("xpack.idp.signing.key", "idp-sign.key")
        .setting("xpack.idp.privileges.application", "elastic-cloud")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.security.authc.realms.file.file.order", "0")
        .setting("xpack.security.authc.realms.native.native.order", "1")
        .setting("xpack.security.authc.realms.saml.cloud-saml.order", "2")
        .setting("xpack.security.authc.realms.saml.cloud-saml.idp.entity_id", "https://idp.test.es.elasticsearch.org/")
        .setting("xpack.security.authc.realms.saml.cloud-saml.idp.metadata.path", "idp-metadata.xml")
        .setting("xpack.security.authc.realms.saml.cloud-saml.sp.entity_id", "ec:123456:abcdefg")
        // This is a dummy one, we simulate the browser and a web app in our tests
        .setting("xpack.security.authc.realms.saml.cloud-saml.sp.acs", "https://sp1.test.es.elasticsearch.org/saml/acs")
        .setting(
            "xpack.security.authc.realms.saml.cloud-saml.attributes.principal",
            "https://idp.test.es.elasticsearch.org/attribute/principal"
        )
        .setting("xpack.security.authc.realms.saml.cloud-saml.attributes.name", "https://idp.test.es.elasticsearch.org/attribute/name")
        .setting("logger.org.elasticsearch.xpack.security.authc.saml", "TRACE")
        .setting("logger.org.elasticsearch.xpack.idp", "TRACE")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .configFile("idp-sign.crt", Resource.fromClasspath("idp-sign.crt"))
        .configFile("idp-sign.key", Resource.fromClasspath("idp-sign.key"))
        .configFile("wildcard_services.json", Resource.fromClasspath("wildcard_services.json"))
        // The SAML SP is preconfigured with the metadata of the IDP
        .configFile("idp-metadata.xml", Resource.fromClasspath("idp-metadata.xml"))
        .user("admin_user", "admin-password")
        .user("idp_admin", "idp-password", "idp_admin", false)
        .user("idp_user", "idp-password", "idp_user", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("idp_admin", new SecureString("idp-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    protected User createUser(String username, SecureString password, String role) throws IOException {
        final User user = new User(
            username,
            new String[] { role },
            username + " in " + getTestName(),
            username + "@test.example.com",
            Map.of(),
            true
        );
        final String endpoint = "/_security/user/" + username;
        final Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        final String body = Strings.format("""
            {
                "username": "%s",
                "full_name": "%s",
                "email": "%s",
                "password": "%s",
                "roles": [ "%s" ]
            }
            """, user.principal(), user.fullName(), user.email(), password.toString(), role);
        request.setJsonEntity(body);
        request.addParameters(Map.of("refresh", "true"));
        request.setOptions(RequestOptions.DEFAULT);
        adminClient().performRequest(request);

        return user;
    }

    protected void deleteUser(String username) throws IOException {
        final String endpoint = "/_security/user/" + username;
        final Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        request.addParameters(Map.of("refresh", "true"));
        request.setOptions(RequestOptions.DEFAULT);
        adminClient().performRequest(request);
    }

    protected void createRole(
        String name,
        Collection<String> clusterPrivileges,
        Collection<RoleDescriptor.IndicesPrivileges> indicesPrivileges,
        Collection<RoleDescriptor.ApplicationResourcePrivileges> applicationPrivileges
    ) throws IOException {
        final RoleDescriptor descriptor = new RoleDescriptor(
            name,
            clusterPrivileges.toArray(String[]::new),
            indicesPrivileges.toArray(RoleDescriptor.IndicesPrivileges[]::new),
            applicationPrivileges.toArray(RoleDescriptor.ApplicationResourcePrivileges[]::new),
            null,
            null,
            Map.of(),
            Map.of()
        );
        final String body = Strings.toString(descriptor);

        final Request request = new Request(HttpPut.METHOD_NAME, "/_security/role/" + name);
        request.setJsonEntity(body);
        adminClient().performRequest(request);
    }

    protected void deleteRole(String name) throws IOException {
        final Request request = new Request(HttpDelete.METHOD_NAME, "/_security/role/" + name);
        adminClient().performRequest(request);
    }

    protected void createApplicationPrivileges(String applicationName, Map<String, Collection<String>> privileges) throws IOException {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), bos);

        builder.startObject();
        builder.startObject(applicationName);
        for (var entry : privileges.entrySet()) {
            builder.startObject(entry.getKey());
            builder.stringListField(ApplicationPrivilegeDescriptor.Fields.ACTIONS.getPreferredName(), entry.getValue());
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        builder.flush();

        final Request request = new Request(HttpPost.METHOD_NAME, "/_security/privilege/");
        request.setJsonEntity(bos.toString(StandardCharsets.UTF_8));
        adminClient().performRequest(request);
    }

    protected void setUserPassword(String username, SecureString password) throws IOException {
        final String endpoint = "/_security/user/" + username + "/_password";
        final Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        final String body = Strings.format("""
            {
                "password": "%s"
            }
            """, password.toString());
        request.setJsonEntity(body);
        request.setOptions(RequestOptions.DEFAULT);
        adminClient().performRequest(request);
    }

    protected SamlServiceProviderIndex.DocumentVersion createServiceProvider(String entityId, Map<String, Object> body) throws IOException {
        // so that we don't hit [SERVICE_UNAVAILABLE/1/state not recovered / initialized]
        ensureGreen("");
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
        return new SamlServiceProviderIndex.DocumentVersion((String) docId, asLong(primaryTerm), asLong(seqNo));
    }

    protected void checkIndexDoc(SamlServiceProviderIndex.DocumentVersion docVersion) throws IOException {
        final Request request = new Request("GET", SamlServiceProviderIndex.INDEX_NAME + "/_doc/" + docVersion.id);
        final Response response = adminClient().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(map.get("_index"), equalTo(SamlServiceProviderIndex.INDEX_NAME));
        assertThat(map.get("_id"), equalTo(docVersion.id));
        assertThat(asLong(map.get("_seq_no")), equalTo(docVersion.seqNo));
        assertThat(asLong(map.get("_primary_term")), equalTo(docVersion.primaryTerm));
    }

    protected Long asLong(Object val) {
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

    protected String encode(String param) {
        return URLEncoder.encode(param, StandardCharsets.UTF_8);
    }

}
