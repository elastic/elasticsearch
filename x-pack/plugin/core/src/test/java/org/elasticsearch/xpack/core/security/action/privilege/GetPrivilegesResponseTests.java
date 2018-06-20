/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Locale;

public class GetPrivilegesResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        ApplicationPrivilege[] privileges = randomArray(6, ApplicationPrivilege[]::new, () ->
            new ApplicationPrivilege(
                randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT),
                randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT),
                randomArray(3, String[]::new, () -> randomAlphaOfLength(3).toLowerCase(Locale.ROOT) + "/*")
            )
        );
        final GetPrivilegesResponse original = new GetPrivilegesResponse(privileges);

        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        final GetPrivilegesResponse copy = new GetPrivilegesResponse();
        copy.readFrom(out.bytes().streamInput());

        assertThat(copy.privileges(), Matchers.equalTo(original.privileges()));
    }

}
