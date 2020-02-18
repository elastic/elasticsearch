/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SerializationTestUtils;
import org.elasticsearch.test.TestMatchers;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderDocument;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndexTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.hamcrest.Matchers.aMapWithSize;
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
        final SamlServiceProviderDocument doc = SamlServiceProviderIndexTests.randomDocument();
        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(doc);
        assertThat(request.validate(), nullValue());
    }

    public void testValidateAcs() {
        final SamlServiceProviderDocument doc = SamlServiceProviderIndexTests.randomDocument();
        doc.acs = "this is not a URL";
        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(doc);
        final ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.validationErrors().get(0), containsString("[acs]"));
        assertThat(validationException.validationErrors().get(0), containsString("valid URL"));
    }

    public void testValidateRequiredFields() {
        final SamlServiceProviderDocument doc = SamlServiceProviderIndexTests.randomDocument();
        doc.name = null;
        doc.entityId = null;
        doc.acs = null;
        doc.created = null;
        doc.lastModified = null;
        doc.attributeNames.principal = null;
        doc.privileges.resource = null;

        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(doc);
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
        final SamlServiceProviderDocument doc = SamlServiceProviderIndexTests.randomDocument();
        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(doc);
        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_7_7_0, Version.CURRENT);
        SerializationTestUtils.assertRoundTrip(request, PutSamlServiceProviderRequest::new, version);
    }

    public void testParseRequestBodySuccessfully() throws Exception {
        final Map<String, Object> fields = new HashMap<>();
        fields.put("name", randomAlphaOfLengthBetween(3, 30));
        fields.put("acs", "https://www." + randomAlphaOfLengthBetween(3, 30) + ".fake/saml/acs");
        fields.put("enabled", randomBoolean());
        fields.put("attributes", Map.of(
            "principal", "urn:oid:0.1." + randomLongBetween(1, 1000),
            "email", "urn:oid:0.2." + randomLongBetween(1001, 2000),
            "name", "urn:oid:0.3." + randomLongBetween(2001, 3000),
            "groups", "urn:oid:0.4." + randomLongBetween(3001, 4000)
        ));
        fields.put("privileges", Map.of(
            "resource", "ece:deployment:" + randomLongBetween(1_000_000, 999_999_999),
            "login", "action:" + randomAlphaOfLengthBetween(3, 12),
            "groups", Map.of("group_name", "role:" + randomAlphaOfLengthBetween(4, 8))
        ));
        fields.put("certificates", Map.of());
        final String entityId = "https://www." + randomAlphaOfLengthBetween(5, 12) + ".app/";
        final PutSamlServiceProviderRequest request = parseRequest(entityId, fields);
        assertThat(request.getDocument().docId, nullValue());
        assertThat(request.getDocument().entityId, equalTo(entityId));
        assertThat(request.getDocument().acs, equalTo(fields.get("acs")));
        assertThat(request.getDocument().enabled, equalTo(fields.get("enabled")));
        assertThat(request.getDocument().privileges.application, nullValue());
        assertThat(request.getDocument().privileges.resource, notNullValue());
        assertThat(request.getDocument().privileges.loginAction, notNullValue());
        assertThat(request.getDocument().privileges.groupActions, aMapWithSize(1));
        assertThat(request.getDocument().privileges.groupActions.keySet(), contains("group_name"));
        assertThat(request.getDocument().attributeNames.principal, startsWith("urn:oid:0.1"));
        assertThat(request.getDocument().attributeNames.email, startsWith("urn:oid:0.2"));
        assertThat(request.getDocument().attributeNames.name, startsWith("urn:oid:0.3"));
        assertThat(request.getDocument().attributeNames.groups, startsWith("urn:oid:0.4"));
        assertThat(request.getDocument().certificates.serviceProviderSigning, emptyIterable());
        assertThat(request.getDocument().certificates.identityProviderSigning, emptyIterable());
        assertThat(request.getDocument().certificates.identityProviderMetadataSigning, emptyIterable());

        assertThat(request.validate(), nullValue());
    }

    public void testParseRequestBodyFailsIfTimestampsAreIncluded() throws Exception {
        final SamlServiceProviderDocument doc = SamlServiceProviderIndexTests.randomDocument();
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
                return PutSamlServiceProviderRequest.fromXContent(entityId, parser);
            }
        }
    }

}
