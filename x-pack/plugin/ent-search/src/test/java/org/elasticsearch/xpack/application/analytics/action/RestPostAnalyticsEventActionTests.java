/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.AbstractRestEnterpriseSearchActionTests;
import org.elasticsearch.xpack.application.EnterpriseSearchBaseRestHandler;

import java.net.InetSocketAddress;
import java.util.Map;

public class RestPostAnalyticsEventActionTests extends AbstractRestEnterpriseSearchActionTests {
    public void testWithNonCompliantLicense() throws Exception {
        checkLicenseForRequest(
            new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.POST)
                .withParams(Map.of("collection_name", "my-collection"))
                .withContent(new BytesArray("{}"), XContentType.JSON)
                .withRemoteAddress(new InetSocketAddress(randomIp(randomBoolean()), randomIntBetween(1, 65535)))
                .build()
        );
    }

    @Override
    protected EnterpriseSearchBaseRestHandler getRestAction(XPackLicenseState licenseState) {
        return new RestPostAnalyticsEventAction(licenseState);
    }
}
