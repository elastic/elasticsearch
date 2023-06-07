/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestMatchers;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderDocument;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderTestUtils;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class PutSamlServiceProviderRequestTests extends ESTestCase {

    public void testValidateSuccessfully() {
        final SamlServiceProviderDocument doc = SamlServiceProviderTestUtils.randomDocument();
        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(doc, randomFrom(RefreshPolicy.values()));
        assertThat(request.validate(), nullValue());
    }

    public void testValidateAcs() {
        final SamlServiceProviderDocument doc = SamlServiceProviderTestUtils.randomDocument();
        doc.acs = "this is not a URL";
        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(doc, randomFrom(RefreshPolicy.values()));
        final ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.validationErrors().get(0), containsString("[acs]"));
        assertThat(validationException.validationErrors().get(0), containsString("valid URL"));
    }

    public void testValidateRequiredFields() {
        final SamlServiceProviderDocument doc = SamlServiceProviderTestUtils.randomDocument();
        doc.name = null;
        doc.entityId = null;
        doc.acs = null;
        doc.created = null;
        doc.lastModified = null;
        doc.attributeNames.principal = null;
        doc.privileges.resource = null;

        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(doc, randomFrom(RefreshPolicy.values()));
        final ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.validationErrors(), hasSize(7));
        assertThat(validationException.validationErrors(), hasItem(containsString("[name]")));
        assertThat(validationException.validationErrors(), hasItem(containsString("[entity_id]")));
        assertThat(validationException.validationErrors(), hasItem(containsString("[acs]")));
        assertThat(validationException.validationErrors(), hasItem(containsString("[created]")));
        assertThat(validationException.validationErrors(), hasItem(containsString("[last_modified]")));
        assertThat(validationException.validationErrors(), hasItem(containsString("[attributes.principal]")));
        assertThat(validationException.validationErrors(), hasItem(containsString("[privileges.resource]")));
    }

    public void testSerialization() throws IOException {
        final SamlServiceProviderDocument doc = SamlServiceProviderTestUtils.randomDocument();
        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(doc, RefreshPolicy.NONE);
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.V_7_7_0,
                TransportVersion.current()
        );
        final PutSamlServiceProviderRequest read = copyWriteable(
            request,
            new NamedWriteableRegistry(List.of()),
            PutSamlServiceProviderRequest::new,
            version
        );
        MatcherAssert.assertThat(
            "Serialized request with version [" + version + "] does not match original object",
            read,
            equalTo(request)
        );
    }

    public void testParseRequestBodySuccessfully() throws Exception {
        final Map<String, Object> fields = new HashMap<>();
        fields.put("name", randomAlphaOfLengthBetween(3, 30));
        fields.put("acs", "https://www." + randomAlphaOfLengthBetween(3, 30) + ".fake/saml/acs");
        fields.put("enabled", randomBoolean());
        fields.put(
            "attributes",
            Map.of(
                "principal",
                "urn:oid:0.1." + randomLongBetween(1, 1000),
                "email",
                "urn:oid:0.2." + randomLongBetween(1001, 2000),
                "name",
                "urn:oid:0.3." + randomLongBetween(2001, 3000),
                "roles",
                "urn:oid:0.4." + randomLongBetween(3001, 4000)
            )
        );
        fields.put(
            "privileges",
            Map.of("resource", "ece:deployment:" + randomLongBetween(1_000_000, 999_999_999), "roles", List.of("role:(.*)"))
        );
        fields.put("certificates", Map.of());
        final String entityId = "https://www." + randomAlphaOfLengthBetween(5, 12) + ".app/";
        final PutSamlServiceProviderRequest request = parseRequest(entityId, fields);
        assertThat(request.getDocument().docId, nullValue());
        assertThat(request.getDocument().entityId, equalTo(entityId));
        assertThat(request.getDocument().acs, equalTo(fields.get("acs")));
        assertThat(request.getDocument().enabled, equalTo(fields.get("enabled")));
        assertThat(request.getDocument().privileges.resource, notNullValue());
        assertThat(request.getDocument().privileges.rolePatterns, contains("role:(.*)"));
        assertThat(request.getDocument().attributeNames.principal, startsWith("urn:oid:0.1"));
        assertThat(request.getDocument().attributeNames.email, startsWith("urn:oid:0.2"));
        assertThat(request.getDocument().attributeNames.name, startsWith("urn:oid:0.3"));
        assertThat(request.getDocument().attributeNames.roles, startsWith("urn:oid:0.4"));
        assertThat(request.getDocument().certificates.serviceProviderSigning, emptyIterable());
        assertThat(request.getDocument().certificates.identityProviderSigning, emptyIterable());
        assertThat(request.getDocument().certificates.identityProviderMetadataSigning, emptyIterable());

        assertThat(request.validate(), nullValue());
    }

    public void testParseRequestBodyFailsIfTimestampsAreIncluded() throws Exception {
        final SamlServiceProviderDocument doc = SamlServiceProviderTestUtils.randomDocument();
        final Map<String, Object> fields = convertToMap(XContentType.JSON.xContent(), Strings.toString(doc), randomBoolean());

        fields.remove("entity_id");
        fields.remove("created");
        fields.remove("last_modified");

        final String field = randomBoolean() ? "created" : "last_modified";
        fields.put(field, System.currentTimeMillis() + randomLongBetween(-100_000, +100_000));

        ElasticsearchParseException exception = expectThrows(ElasticsearchParseException.class, () -> parseRequest(doc.entityId, fields));
        assertThat(exception, TestMatchers.throwableWithMessage("Field [" + field + "] may not be specified in a request"));
    }

    private PutSamlServiceProviderRequest parseRequest(String entityId, Map<String, Object> fields) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.map(fields);
            try (XContentParser parser = createParser(shuffleXContent(builder))) {
                return PutSamlServiceProviderRequest.fromXContent(entityId, randomFrom(RefreshPolicy.values()), parser);
            }
        }
    }

}
