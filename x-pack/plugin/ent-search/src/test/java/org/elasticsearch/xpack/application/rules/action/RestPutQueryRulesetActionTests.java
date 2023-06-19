/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.AbstractRestEnterpriseSearchActionTests;
import org.elasticsearch.xpack.application.EnterpriseSearchBaseRestHandler;

import java.util.Map;

public class RestPutQueryRulesetActionTests extends AbstractRestEnterpriseSearchActionTests {
    public void testWithNonCompliantLicense() throws Exception {
        checkLicenseForRequest(
            new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.PUT)
                .withParams(Map.of("ruleset_id", "ruleset-id"))
                .withContent(new BytesArray("""
                    {
                      "ruleset_id": "ruleset-id",
                      "rules": [
                        {
                          "rule_id": "query-rule-id",
                          "type": "pinned",
                          "criteria": [
                            {
                              "type": "exact",
                              "metadata": "query_string",
                              "value": "elastic"
                            }
                          ],
                          "actions":
                            {
                              "ids": [
                                "id1",
                                "id2"
                              ]
                            }
                        }
                      ]
                    }
                    """), XContentType.JSON)
                .build()
        );
    }

    @Override
    protected EnterpriseSearchBaseRestHandler getRestAction(XPackLicenseState licenseState) {
        return new RestPutQueryRulesetAction(licenseState);
    }
}
