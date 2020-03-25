/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.rolemapping;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class PutRoleMappingRequestTests extends ESTestCase {

    private PutRoleMappingRequestBuilder builder;

    @Before
    public void setupBuilder() {
        final ElasticsearchClient client = Mockito.mock(ElasticsearchClient.class);
        builder = new PutRoleMappingRequestBuilder(client);
    }

    public void testValidateMissingName() throws Exception {
        final PutRoleMappingRequest request = builder
                .roles("superuser")
                .expression(Mockito.mock(RoleMapperExpression.class))
                .request();
        assertValidationFailure(request, "name");
    }

    public void testValidateMissingRoles() throws Exception {
        final PutRoleMappingRequest request = builder
                .name("test")
                .expression(Mockito.mock(RoleMapperExpression.class))
                .request();
        assertValidationFailure(request, "roles");
    }

    public void testValidateMissingRules() throws Exception {
        final PutRoleMappingRequest request = builder
                .name("test")
                .roles("superuser")
                .request();
        assertValidationFailure(request, "rules");
    }

    public void testValidateMetadataKeys() throws Exception {
        final PutRoleMappingRequest request = builder
                .name("test")
                .roles("superuser")
                .expression(Mockito.mock(RoleMapperExpression.class))
                .metadata(Collections.singletonMap("_secret", false))
                .request();
        assertValidationFailure(request, "metadata key");
    }

    private void assertValidationFailure(PutRoleMappingRequest request, String expectedMessage) {
        final ValidationException ve = request.validate();
        assertThat(ve, notNullValue());
        assertThat(ve.getMessage(), containsString(expectedMessage));
    }

}
