/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.Version;
import org.elasticsearch.test.SerializationTestUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;

import java.io.IOException;

public class DeleteSamlServiceProviderRequestTests extends IdpSamlTestCase {

    public void testSerialization() throws IOException {
        final DeleteSamlServiceProviderRequest request = new DeleteSamlServiceProviderRequest(randomAlphaOfLengthBetween(1, 100));
        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_7_7_0, Version.CURRENT);
        SerializationTestUtils.assertRoundTrip(request, DeleteSamlServiceProviderRequest::new, version);
    }

}
