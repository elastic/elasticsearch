/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
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

        final GetPrivilegesResponse copy = new GetPrivilegesResponse(out.bytes().streamInput());

        assertThat(copy.privileges(), Matchers.equalTo(original.privileges()));
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
        return new GetPrivilegesResponse(application);
    }

}
