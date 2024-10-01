/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;

import java.util.List;
import java.util.Set;

public abstract class AbstractRoleMappingStore extends AbstractRoleMapperClearRealmCache {
    abstract void putRoleMapping(PutRoleMappingRequest request, ActionListener<Boolean> listener);

    abstract void deleteRoleMapping(DeleteRoleMappingRequest request, ActionListener<Boolean> listener);

    abstract void getRoleMappings(Set<String> names, ActionListener<List<ExpressionRoleMapping>> listener);
}
