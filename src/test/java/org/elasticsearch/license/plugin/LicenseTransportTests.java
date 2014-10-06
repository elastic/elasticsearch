/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseUtils;
import org.elasticsearch.license.licensor.tools.KeyPairGeneratorTool;
import org.elasticsearch.license.plugin.action.get.GetLicenseAction;
import org.elasticsearch.license.plugin.action.get.GetLicenseRequest;
import org.elasticsearch.license.plugin.action.get.GetLicenseResponse;
import org.elasticsearch.license.plugin.action.get.TransportGetLicenseAction;
import org.elasticsearch.license.plugin.action.put.PutLicenseAction;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.elasticsearch.license.plugin.action.put.TransportPutLicenseAction;
import org.elasticsearch.license.plugin.core.LicensesMetaData;
import org.elasticsearch.license.plugin.rest.RestGetLicenseAction;
import org.elasticsearch.license.plugin.rest.RestPutLicenseAction;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ClusterScope(scope = SUITE, numDataNodes = 10)
public class LicenseTransportTests extends ElasticsearchIntegrationTest {


    private static String pubKeyPath = null;
    private static String priKeyPath = null;
    private static String keyPass = null;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("plugins.load_classpath_plugins", false)
                .put("plugin.types", LicensePlugin.class.getName())
                .build();
    }

    @BeforeClass
    public static void setup() throws IOException {

        // Generate temp KeyPair spec
        File privateKeyFile = File.createTempFile("privateKey", ".key");
        File publicKeyFile = File.createTempFile("publicKey", ".key");
        LicenseTransportTests.pubKeyPath = publicKeyFile.getAbsolutePath();
        LicenseTransportTests.priKeyPath = privateKeyFile.getAbsolutePath();
        assert privateKeyFile.delete();
        assert publicKeyFile.delete();
        String keyPass = "password";
        LicenseTransportTests.keyPass = keyPass;

        // Generate keyPair
        String[] args = new String[6];
        args[0] = "--publicKeyPath";
        args[1] = LicenseTransportTests.pubKeyPath;
        args[2] = "--privateKeyPath";
        args[3] = LicenseTransportTests.priKeyPath;
        args[4] = "--keyPass";
        args[5] = LicenseTransportTests.keyPass;
        KeyPairGeneratorTool.main(args);
    }

    @Test
    public void testEmptyGetLicense() throws Exception {
        final ActionFuture<GetLicenseResponse> getLicenseFuture = licenseGetAction().execute(new GetLicenseRequest());

        final GetLicenseResponse getLicenseResponse = getLicenseFuture.get();

        //TODO
        //assertThat(getLicenseResponse.licenses(), nullValue());
    }

    @Test
    public void testPutLicense() throws ParseException, ExecutionException, InterruptedException, IOException {

        Map<ESLicenses.FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-12-13");
        map.put(ESLicenses.FeatureType.SHIELD, featureAttributes);

        String licenseString = TestUtils.generateESLicenses(map);


        String[] args = new String[8];
        args[0] = "--license";
        args[1] = licenseString;
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--privateKeyPath";
        args[5] = priKeyPath;
        args[6] = "--keyPass";
        args[7] = keyPass;

        String licenseOutput = TestUtils.runLicenseGenerationTool(args);

        PutLicenseRequest putLicenseRequest = new PutLicenseRequest();
        //putLicenseRequest.license(licenseString);
        final ESLicenses putLicenses = LicenseUtils.readLicensesFromString(licenseOutput);
        putLicenseRequest.license(putLicenses);
        LicenseUtils.printLicense(putLicenses);
        ensureGreen();

        final ActionFuture<PutLicenseResponse> putLicenseFuture = licensePutAction().execute(putLicenseRequest);

        final PutLicenseResponse putLicenseResponse = putLicenseFuture.get();

        assertThat(putLicenseResponse.isAcknowledged(), equalTo(true));

        final ActionFuture<GetLicenseResponse> getLicenseFuture = licenseGetAction().execute(new GetLicenseRequest());

        final GetLicenseResponse getLicenseResponse = getLicenseFuture.get();

        LicenseUtils.printLicense(getLicenseResponse.licenses());
    }

    public TransportGetLicenseAction licenseGetAction() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(TransportGetLicenseAction.class);
    }

    public TransportPutLicenseAction licensePutAction() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(TransportPutLicenseAction.class);
    }
}
