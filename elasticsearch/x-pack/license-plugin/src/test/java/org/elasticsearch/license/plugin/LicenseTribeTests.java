/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseAction;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseRequest;
import org.elasticsearch.license.plugin.action.get.GetLicenseAction;
import org.elasticsearch.license.plugin.action.get.GetLicenseRequest;
import org.elasticsearch.license.plugin.action.put.PutLicenseAction;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.xpack.TribeTransportTestCase;

import static org.elasticsearch.license.plugin.TestUtils.generateSignedLicense;

public class LicenseTribeTests extends TribeTransportTestCase {

    @Override
    protected void verifyActionOnDataNode(Client dataNodeClient) throws Exception {
        dataNodeClient.execute(GetLicenseAction.INSTANCE, new GetLicenseRequest()).get();
        dataNodeClient.execute(PutLicenseAction.INSTANCE, new PutLicenseRequest()
                .license(generateSignedLicense(TimeValue.timeValueHours(1))));
        dataNodeClient.execute(DeleteLicenseAction.INSTANCE, new DeleteLicenseRequest());
    }

    @Override
    protected void verifyActionOnTribeNode(Client tribeClient) {
        failAction(tribeClient, GetLicenseAction.INSTANCE);
        failAction(tribeClient, PutLicenseAction.INSTANCE);
        failAction(tribeClient, DeleteLicenseAction.INSTANCE);
    }
}
