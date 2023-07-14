/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.application.AbstractRestEnterpriseSearchActionTests;
import org.elasticsearch.xpack.application.EnterpriseSearchBaseRestHandler;
import org.elasticsearch.xpack.application.rules.action.RestListQueryRulesetsAction;

import java.util.Map;

public class RestListQueryRulesetTests extends AbstractRestEnterpriseSearchActionTests {
    public void testWithNonCompliantLicense() throws Exception {
        checkLicenseForRequest(
            new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(Map.of("name", "my-query-ruleset")).build()
        );
    }

    @Override
    protected EnterpriseSearchBaseRestHandler getRestAction(XPackLicenseState licenseState) {
        return new RestListQueryRulesetsAction(licenseState);
    }

}
