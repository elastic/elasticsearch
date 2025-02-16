/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A request for checking a user's privileges
 */
public class HasPrivilegesRequest extends ActionRequest implements UserRequest {

    private String username;
    private String[] clusterPrivileges;
    private IndicesPrivileges[] indexPrivileges;
    private ApplicationResourcePrivileges[] applicationPrivileges;
    // this is hard-coded for now, but it doesn't have to be
    private final boolean runDetailedCheck = true;

    public HasPrivilegesRequest() {}

    public HasPrivilegesRequest(StreamInput in) throws IOException {
        super(in);
        this.username = in.readString();
        this.clusterPrivileges = in.readStringArray();
        int indexSize = in.readVInt();
        indexPrivileges = new IndicesPrivileges[indexSize];
        for (int i = 0; i < indexSize; i++) {
            indexPrivileges[i] = new IndicesPrivileges(in);
        }
        applicationPrivileges = in.readArray(ApplicationResourcePrivileges::new, ApplicationResourcePrivileges[]::new);
    }

    public AuthorizationEngine.PrivilegesToCheck getPrivilegesToCheck() {
        return new AuthorizationEngine.PrivilegesToCheck(clusterPrivileges, indexPrivileges, applicationPrivileges, runDetailedCheck);
    }

    @Override
    public ActionRequestValidationException validate() {
        assert getPrivilegesToCheck().runDetailedCheck();
        return getPrivilegesToCheck().validate(null);
    }

    /**
     * @return the username that this request applies to.
     */
    public String username() {
        return username;
    }

    /**
     * Set the username that the request applies to. Must not be {@code null}
     */
    public void username(String username) {
        this.username = username;
    }

    @Override
    public String[] usernames() {
        return new String[] { username };
    }

    public IndicesPrivileges[] indexPrivileges() {
        return indexPrivileges;
    }

    public String[] clusterPrivileges() {
        return clusterPrivileges;
    }

    public ApplicationResourcePrivileges[] applicationPrivileges() {
        return applicationPrivileges;
    }

    public void indexPrivileges(IndicesPrivileges... privileges) {
        IndicesPrivileges[] newPrivileges = new IndicesPrivileges[privileges.length];
        for (int i = 0; i < privileges.length; i++) {
            IndicesPrivileges currentPriv = privileges[i];
            IndicesPrivileges.Builder builder = IndicesPrivileges.builder(privileges[i]);
            builder.indices((String[]) null);
            List<String> updatedIndexPatterns = new ArrayList<>();
            for (String indexPatternRequested : currentPriv.getIndices()) {
                Tuple<String, String> split = IndexNameExpressionResolver.splitSelectorExpression(indexPatternRequested);
                String indexNameNoSelector = split.v1();
                String selectorAsString = split.v2();
                if (selectorAsString == null) {
                    assert indexPatternRequested.equals(indexNameNoSelector);
                    updatedIndexPatterns.add(indexNameNoSelector); // add as-is, no selector
                } else {
                    IndexComponentSelector selector = IndexComponentSelector.getByKey(selectorAsString);
                    switch (selector) {
                        case DATA:
                            updatedIndexPatterns.add(indexNameNoSelector); // strip the selector
                            break;
                        case FAILURES:
                            updatedIndexPatterns.add(indexPatternRequested); // add as-is, keep selector in name
                            break;
                        case ALL_APPLICABLE:
                            updatedIndexPatterns.add(indexNameNoSelector); // add with no selector for data
                            updatedIndexPatterns.add(
                                IndexNameExpressionResolver.combineSelector(indexNameNoSelector, IndexComponentSelector.FAILURES)
                            ); // add with failure selector
                            break;
                        default:
                            throw new IllegalArgumentException(
                                "Unknown index component selector ["
                                    + selectorAsString
                                    + "], available options are: "
                                    + IndexComponentSelector.values()
                            );

                    }
                }
                builder.indices(updatedIndexPatterns);
                newPrivileges[i] = builder.build();
            }
        }

        this.indexPrivileges = newPrivileges;
    }

    public void clusterPrivileges(String... privileges) {
        this.clusterPrivileges = privileges;
    }

    public void applicationPrivileges(ApplicationResourcePrivileges... appPrivileges) {
        this.applicationPrivileges = appPrivileges;
    }

    public void privilegesToCheck(AuthorizationEngine.PrivilegesToCheck privilegesToCheck) {
        assert privilegesToCheck.runDetailedCheck() == runDetailedCheck;
        clusterPrivileges(privilegesToCheck.cluster());
        indexPrivileges(privilegesToCheck.index());
        applicationPrivileges(privilegesToCheck.application());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
        out.writeStringArray(clusterPrivileges);
        out.writeVInt(indexPrivileges.length);
        for (IndicesPrivileges priv : indexPrivileges) {
            priv.writeTo(out);
        }
        out.writeArray(ApplicationResourcePrivileges::write, applicationPrivileges);
    }
}
