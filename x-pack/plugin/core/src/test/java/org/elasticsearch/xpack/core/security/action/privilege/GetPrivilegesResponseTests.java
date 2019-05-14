/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;

public class GetPrivilegesResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final GetPrivilegesResponse original = randomResponse();

        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        final GetPrivilegesResponse copy = new GetPrivilegesResponse();
        copy.readFrom(out.bytes().streamInput());

        assertThat(copy.getClusterPrivileges(), Matchers.equalTo(original.getClusterPrivileges()));
        assertThat(copy.getIndexPrivileges(), Matchers.equalTo(original.getIndexPrivileges()));
        assertThat(copy.applicationPrivileges(), Matchers.equalTo(original.applicationPrivileges()));
    }

    public void testSerializationBeforeV72() throws IOException {
        final Version version = VersionUtils.randomVersionBetween(random(), null, Version.V_7_1_0);
        final GetPrivilegesResponse original = randomResponse();

        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        original.writeTo(out);

        final GetPrivilegesResponse copy = new GetPrivilegesResponse();
        final StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        copy.readFrom(in);

        assertThat(copy.getClusterPrivileges(), Matchers.emptyArray());
        assertThat(copy.getIndexPrivileges(), Matchers.emptyArray());
        assertThat(copy.applicationPrivileges(), Matchers.equalTo(original.applicationPrivileges()));
    }

    private static GetPrivilegesResponse randomResponse() {
        ApplicationPrivilegeDescriptor[] application = randomArray(6, ApplicationPrivilegeDescriptor[]::new, () ->
            new ApplicationPrivilegeDescriptor(
                randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT),
                randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT),
                Sets.newHashSet(randomArray(3, String[]::new, () -> randomAlphaOfLength(3).toLowerCase(Locale.ROOT) + "/*")),
                Collections.emptyMap()
            )
        );
        final String[] cluster = randomArray(3, 12, String[]::new, () -> randomAlphaOfLengthBetween(2, 8));
        final String[] index = randomArray(3, 12, String[]::new, () -> randomAlphaOfLengthBetween(2, 8));

        return new GetPrivilegesResponse(cluster, index, application);
    }

}
