/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class DeleteSamlServiceProviderRequestTests extends IdpSamlTestCase {

    public void testSerialization() throws IOException {
        final DeleteSamlServiceProviderRequest request = new DeleteSamlServiceProviderRequest(
            randomAlphaOfLengthBetween(1, 100),
            randomFrom(WriteRequest.RefreshPolicy.values())
        );
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_8_0_0,
            TransportVersion.current()
        );
        final DeleteSamlServiceProviderRequest read = copyWriteable(
            request,
            new NamedWriteableRegistry(List.of()),
            DeleteSamlServiceProviderRequest::new,
            version
        );
        MatcherAssert.assertThat(
            "Serialized request with version [" + version + "] does not match original object",
            read,
            equalTo(request)
        );
    }

}
