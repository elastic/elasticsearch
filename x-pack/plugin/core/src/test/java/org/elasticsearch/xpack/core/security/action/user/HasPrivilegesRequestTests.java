/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

public class HasPrivilegesRequestTests extends ESTestCase {

    public void testSerializationCurrentVersion() throws IOException {
        final HasPrivilegesRequest original = randomRequest();
        final Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        final HasPrivilegesRequest copy = serializeAndDeserialize(original, version);

        assertThat(copy.username(), equalTo(original.username()));
        assertThat(copy.clusterPrivileges(), equalTo(original.clusterPrivileges()));
        assertThat(copy.indexPrivileges(), equalTo(original.indexPrivileges()));
        assertThat(copy.applicationPrivileges(), equalTo(original.applicationPrivileges()));
    }

    public void testValidateNullPrivileges() {
        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), hasItem("clusterPrivileges must not be null"));
        assertThat(exception.validationErrors(), hasItem("indexPrivileges must not be null"));
        assertThat(exception.validationErrors(), hasItem("applicationPrivileges must not be null"));
    }

    public void testValidateEmptyPrivileges() {
        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.clusterPrivileges(new String[0]);
        request.indexPrivileges(new IndicesPrivileges[0]);
        request.applicationPrivileges(new ApplicationResourcePrivileges[0]);
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), hasItem("must specify at least one privilege"));
    }

    public void testValidateNoWildcardApplicationPrivileges() {
        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.clusterPrivileges(new String[0]);
        request.indexPrivileges(new IndicesPrivileges[0]);
        request.applicationPrivileges(
            new ApplicationResourcePrivileges[] {
                ApplicationResourcePrivileges.builder().privileges("read").application("*").resources("item/1").build() }
        );
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), hasItem("Application names may not contain '*' (found '*')"));
    }

    private HasPrivilegesRequest serializeAndDeserialize(HasPrivilegesRequest original, Version version) throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        original.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        final HasPrivilegesRequest copy = new HasPrivilegesRequest(in);
        assertThat(in.read(), equalTo(-1));
        return copy;
    }

    private HasPrivilegesRequest randomRequest() {
        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(randomAlphaOfLength(8));

        final List<String> clusterPrivileges = randomSubsetOf(
            Arrays.asList(
                ClusterPrivilegeResolver.MONITOR,
                ClusterPrivilegeResolver.MANAGE,
                ClusterPrivilegeResolver.MANAGE_ML,
                ClusterPrivilegeResolver.MANAGE_SECURITY,
                ClusterPrivilegeResolver.MANAGE_PIPELINE,
                ClusterPrivilegeResolver.ALL
            )
        ).stream().map(p -> p.name()).collect(Collectors.toList());
        request.clusterPrivileges(clusterPrivileges.toArray(Strings.EMPTY_ARRAY));

        IndicesPrivileges[] indicesPrivileges = new IndicesPrivileges[randomInt(5)];
        for (int i = 0; i < indicesPrivileges.length; i++) {
            indicesPrivileges[i] = IndicesPrivileges.builder()
                .privileges(randomFrom("read", "write", "create", "delete", "all"))
                .indices(randomAlphaOfLengthBetween(2, 8) + (randomBoolean() ? "*" : ""))
                .build();
        }
        request.indexPrivileges(indicesPrivileges);

        final ApplicationResourcePrivileges[] appPrivileges = new ApplicationResourcePrivileges[randomInt(5)];
        for (int i = 0; i < appPrivileges.length; i++) {
            appPrivileges[i] = ApplicationResourcePrivileges.builder()
                .application(randomAlphaOfLengthBetween(3, 8))
                .resources(randomAlphaOfLengthBetween(5, 7) + (randomBoolean() ? "*" : ""))
                .privileges(generateRandomStringArray(6, 7, false, false))
                .build();
        }
        request.applicationPrivileges(appPrivileges);
        return request;
    }

}
