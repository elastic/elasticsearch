/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.rolemapping;

import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_METADATA_FLAG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutRoleMappingRequestTests extends ESTestCase {

    private PutRoleMappingRequestBuilder builder;

    @Before
    public void setupBuilder() {
        final ElasticsearchClient client = Mockito.mock(ElasticsearchClient.class);
        builder = new PutRoleMappingRequestBuilder(client);
    }

    public void testValidateMissingName() throws Exception {
        final PutRoleMappingRequest request = builder.roles("superuser").expression(Mockito.mock(RoleMapperExpression.class)).request();
        assertValidationFailure(request, "name");
    }

    public void testValidateMissingRoles() throws Exception {
        final PutRoleMappingRequest request = builder.name("test").expression(Mockito.mock(RoleMapperExpression.class)).request();
        assertValidationFailure(request, "roles");
    }

    public void testValidateMissingRules() throws Exception {
        final PutRoleMappingRequest request = builder.name("test").roles("superuser").request();
        assertValidationFailure(request, "rules");
    }

    public void testValidateMetadataKeys() throws Exception {
        final PutRoleMappingRequest request = builder.name("test")
            .roles("superuser")
            .expression(Mockito.mock(RoleMapperExpression.class))
            .metadata(Collections.singletonMap("_secret", false))
            .request();
        assertValidationFailure(request, "metadata key");
    }

    public void testValidateReadyOnlyMetadataKey() {
        assertValidationFailure(
            builder.name("test")
                .roles("superuser")
                .expression(Mockito.mock(RoleMapperExpression.class))
                .metadata(Map.of("_secret", false, ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_METADATA_FLAG, true))
                .request(),
            "metadata contains ["
                + READ_ONLY_ROLE_MAPPING_METADATA_FLAG
                + "] flag. You cannot create or update role-mappings with a read-only flag"
        );

        assertValidationFailure(
            builder.name("test")
                .roles("superuser")
                .expression(Mockito.mock(RoleMapperExpression.class))
                .metadata(Map.of(ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_METADATA_FLAG, true))
                .request(),
            "metadata contains ["
                + READ_ONLY_ROLE_MAPPING_METADATA_FLAG
                + "] flag. You cannot create or update role-mappings with a read-only flag"
        );
    }

    public void testValidateMetadataKeySkipped() {
        assertThat(
            builder.name("test")
                .roles("superuser")
                .expression(Mockito.mock(RoleMapperExpression.class))
                .metadata(Map.of("_secret", false, ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_METADATA_FLAG, true))
                .request()
                .validate(false),
            is(nullValue())
        );

        assertThat(
            builder.name("test")
                .roles("superuser")
                .expression(Mockito.mock(RoleMapperExpression.class))
                .metadata(Map.of(ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_METADATA_FLAG, true))
                .request()
                .validate(false),
            is(nullValue())
        );

        assertThat(
            builder.name("test")
                .roles("superuser")
                .expression(Mockito.mock(RoleMapperExpression.class))
                .metadata(Map.of("_secret", false))
                .request()
                .validate(false),
            is(nullValue())
        );
    }

    private void assertValidationFailure(PutRoleMappingRequest request, String expectedMessage) {
        final ValidationException ve = request.validate();
        assertThat(ve, notNullValue());
        assertThat(ve.getMessage(), containsString(expectedMessage));
    }

}
