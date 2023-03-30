/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

public class LazyRoleDescriptor implements RoleDescriptor {

    private final String name;
    private BytesReference bytesReference;
    private RoleDescriptor delegate;

    public LazyRoleDescriptor(String name, BytesReference bytesReference) {
        this.name = name;
        this.bytesReference = bytesReference;
        this.delegate = null;
    }

    private RoleDescriptor initDelegate() {
        if (delegate == null) {
            assert bytesReference != null;
            try {
                delegate = RoleDescriptor.parse(name, bytesReference, true, XContentType.JSON);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            bytesReference = null;
        }
        return delegate;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.rawValue(bytesReference.streamInput(), XContentType.JSON);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        initDelegate().writeTo(out);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String[] getClusterPrivileges() {
        return initDelegate().getClusterPrivileges();
    }

    @Override
    public ConfigurableClusterPrivilege[] getConditionalClusterPrivileges() {
        return initDelegate().getConditionalClusterPrivileges();
    }

    @Override
    public IndicesPrivileges[] getIndicesPrivileges() {
        return initDelegate().getIndicesPrivileges();
    }

    @Override
    public RemoteIndicesPrivileges[] getRemoteIndicesPrivileges() {
        return initDelegate().getRemoteIndicesPrivileges();
    }

    @Override
    public boolean hasRemoteIndicesPrivileges() {
        return initDelegate().hasRemoteIndicesPrivileges();
    }

    @Override
    public ApplicationResourcePrivileges[] getApplicationPrivileges() {
        return initDelegate().getApplicationPrivileges();
    }

    @Override
    public boolean hasClusterPrivileges() {
        return initDelegate().hasClusterPrivileges();
    }

    @Override
    public boolean hasApplicationPrivileges() {
        return initDelegate().hasApplicationPrivileges();
    }

    @Override
    public boolean hasConfigurableClusterPrivileges() {
        return initDelegate().hasConfigurableClusterPrivileges();
    }

    @Override
    public boolean hasRunAs() {
        return initDelegate().hasRunAs();
    }

    @Override
    public String[] getRunAs() {
        return initDelegate().getRunAs();
    }

    @Override
    public Map<String, Object> getMetadata() {
        return initDelegate().getMetadata();
    }

    @Override
    public Map<String, Object> getTransientMetadata() {
        return initDelegate().getTransientMetadata();
    }

    @Override
    public boolean isUsingDocumentOrFieldLevelSecurity() {
        return initDelegate().isUsingDocumentOrFieldLevelSecurity();
    }

    @Override
    public boolean isEmpty() {
        return initDelegate().isEmpty();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params, boolean docCreation) throws IOException {
        return initDelegate().toXContent(builder, params, docCreation);
    }
}
