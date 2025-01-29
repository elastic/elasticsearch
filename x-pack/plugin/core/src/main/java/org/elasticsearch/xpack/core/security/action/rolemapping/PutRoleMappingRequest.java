/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.rolemapping;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionParser;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_METADATA_FLAG;

/**
 * Request object for adding/updating a role-mapping to the native store
 *
 * see org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore
 */
public class PutRoleMappingRequest extends ActionRequest implements WriteRequest<PutRoleMappingRequest> {

    private String name = null;
    private boolean enabled = true;
    private List<String> roles = Collections.emptyList();
    private List<TemplateRoleName> roleTemplates = Collections.emptyList();
    private RoleMapperExpression rules = null;
    private Map<String, Object> metadata = Collections.emptyMap();
    private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

    public PutRoleMappingRequest(StreamInput in) throws IOException {
        super(in);
        this.name = in.readString();
        this.enabled = in.readBoolean();
        this.roles = in.readStringCollectionAsList();
        this.roleTemplates = in.readCollectionAsList(TemplateRoleName::new);
        this.rules = ExpressionParser.readExpression(in);
        this.metadata = in.readGenericMap();
        this.refreshPolicy = RefreshPolicy.readFrom(in);
    }

    public PutRoleMappingRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        return validate(true);
    }

    public ActionRequestValidationException validate(boolean validateMetadata) {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("role-mapping name is missing", validationException);
        }
        if (roles.isEmpty() && roleTemplates.isEmpty()) {
            validationException = addValidationError("role-mapping roles or role-templates are missing", validationException);
        }
        if (roles.size() > 0 && roleTemplates.size() > 0) {
            validationException = addValidationError("role-mapping cannot have both roles and role-templates", validationException);
        }
        if (rules == null) {
            validationException = addValidationError("role-mapping rules are missing", validationException);
        }
        if (validateMetadata && MetadataUtils.containsReservedMetadata(metadata)) {
            if (metadata.containsKey(READ_ONLY_ROLE_MAPPING_METADATA_FLAG)) {
                validationException = addValidationError(
                    "metadata contains ["
                        + READ_ONLY_ROLE_MAPPING_METADATA_FLAG
                        + "] flag. You cannot create or update role-mappings with a read-only flag",
                    validationException
                );
            } else {
                validationException = addValidationError(
                    "metadata keys may not start with [" + MetadataUtils.RESERVED_PREFIX + "]",
                    validationException
                );
            }
        }
        return validationException;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<String> getRoles() {
        return Collections.unmodifiableList(roles);
    }

    public List<TemplateRoleName> getRoleTemplates() {
        return Collections.unmodifiableList(roleTemplates);
    }

    public void setRoles(List<String> roles) {
        this.roles = new ArrayList<>(roles);
    }

    public void setRoleTemplates(List<TemplateRoleName> templates) {
        this.roleTemplates = new ArrayList<>(templates);
    }

    public RoleMapperExpression getRules() {
        return rules;
    }

    public void setRules(RoleMapperExpression expression) {
        this.rules = expression;
    }

    @Override
    public PutRoleMappingRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}, the default),
     * wait for a refresh ({@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes
     * entirely ({@linkplain RefreshPolicy#NONE}).
     */
    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = Objects.requireNonNull(metadata);
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeBoolean(enabled);
        out.writeStringCollection(roles);
        out.writeCollection(roleTemplates);
        ExpressionParser.writeExpression(rules, out);
        out.writeGenericMap(metadata);
        refreshPolicy.writeTo(out);
    }

    public ExpressionRoleMapping getMapping() {
        return new ExpressionRoleMapping(name, rules, roles, roleTemplates, metadata, enabled);
    }
}
