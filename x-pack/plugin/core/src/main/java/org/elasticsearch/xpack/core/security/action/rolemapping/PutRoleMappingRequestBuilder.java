/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.rolemapping;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Builder for requests to add/update a role-mapping to the native store
 *
 * see org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore
 */
public class PutRoleMappingRequestBuilder extends ActionRequestBuilder<PutRoleMappingRequest, PutRoleMappingResponse> implements
        WriteRequestBuilder<PutRoleMappingRequestBuilder> {

    public PutRoleMappingRequestBuilder(ElasticsearchClient client) {
        super(client, PutRoleMappingAction.INSTANCE, new PutRoleMappingRequest());
    }

    /**
     * Populate the put role request from the source and the role's name
     */
    public PutRoleMappingRequestBuilder source(String name, BytesReference source,
                                               XContentType xContentType) throws IOException {
        ExpressionRoleMapping mapping = ExpressionRoleMapping.parse(name, source, xContentType);
        request.setName(name);
        request.setEnabled(mapping.isEnabled());
        request.setRoles(mapping.getRoles());
        request.setRoleTemplates(mapping.getRoleTemplates());
        request.setRules(mapping.getExpression());
        request.setMetadata(mapping.getMetadata());
        return this;
    }

    public PutRoleMappingRequestBuilder name(String name) {
        request.setName(name);
        return this;
    }

    public PutRoleMappingRequestBuilder roles(String... roles) {
        request.setRoles(Arrays.asList(roles));
        return this;
    }
    public PutRoleMappingRequestBuilder roleTemplates(TemplateRoleName... templates) {
        request.setRoleTemplates(Arrays.asList(templates));
        return this;
    }

    public PutRoleMappingRequestBuilder expression(RoleMapperExpression expression) {
        request.setRules(expression);
        return this;
    }

    public PutRoleMappingRequestBuilder enabled(boolean enabled) {
        request.setEnabled(enabled);
        return this;
    }

    public PutRoleMappingRequestBuilder metadata(Map<String, Object> metadata) {
        request.setMetadata(metadata);
        return this;
    }
}
