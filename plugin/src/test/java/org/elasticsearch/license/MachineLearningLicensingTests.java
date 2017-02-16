/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.TestXPackTransportClient;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.ml.action.CloseJobAction;
import org.elasticsearch.xpack.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.ml.client.MachineLearningClient;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.persistent.PersistentActionResponse;
import org.elasticsearch.xpack.persistent.RemovePersistentTaskAction;
import org.junit.Before;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class MachineLearningLicensingTests extends BaseMlIntegTestCase {

    @Before
    public void resetLicensing() {
        enableLicensing();

        ensureStableCluster(1);
        ensureYellow();
    }
    
    public void testMachineLearningPutJobActionRestricted() throws Exception {
        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);
        // test that license restricted apis do not work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PutJobAction.Response> listener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).putJob(new PutJobAction.Request(createJob("foo").build(true, "foo")), listener);
            listener.actionGet();
            fail("put job action should not be enabled!");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
            assertThat(e.getMessage(), containsString("non-compliant"));
            assertThat(e.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasItem(XPackPlugin.MACHINE_LEARNING));
        }

        // Pick a license that does allow machine learning
        mode = randomValidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(true);
        // test that license restricted apis do now work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PutJobAction.Response> listener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).putJob(new PutJobAction.Request(createJob("foo").build(true, "foo")), listener);
            PutJobAction.Response response = listener.actionGet();
            assertNotNull(response);
        }
    }

    public void testMachineLearningOpenJobActionRestricted() throws Exception {

        assertMLAllowed(true);
        // test that license restricted apis do now work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PutJobAction.Response> putJobListener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).putJob(new PutJobAction.Request(createJob("foo").build(true, "foo")), putJobListener);
            PutJobAction.Response response = putJobListener.actionGet();
            assertNotNull(response);
        }
        
        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);
        // test that license restricted apis do not work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PersistentActionResponse> listener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).openJob(new OpenJobAction.Request("foo"), listener);
            listener.actionGet();
            fail("open job action should not be enabled!");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
            assertThat(e.getMessage(), containsString("non-compliant"));
            assertThat(e.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasItem(XPackPlugin.MACHINE_LEARNING));
        }

        // Pick a license that does allow machine learning
        mode = randomValidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(true);
        // test that license restricted apis do now work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PersistentActionResponse> listener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).openJob(new OpenJobAction.Request("foo"), listener);
            PersistentActionResponse response = listener.actionGet();
            assertNotNull(response);
        }
    }

    public void testMachineLearningPutDatafeedActionRestricted() throws Exception {

        assertMLAllowed(true);
        // test that license restricted apis do now work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PutJobAction.Response> putJobListener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).putJob(new PutJobAction.Request(createJob("foo").build(true, "foo")), putJobListener);
            PutJobAction.Response putJobResponse = putJobListener.actionGet();
            assertNotNull(putJobResponse);
        }
        
        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);
        // test that license restricted apis do not work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PutDatafeedAction.Response> listener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).putDatafeed(
                    new PutDatafeedAction.Request(createDatafeed("foobar", "foo", Collections.singletonList("foo"))), listener);
            listener.actionGet();
            fail("put datafeed action should not be enabled!");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
            assertThat(e.getMessage(), containsString("non-compliant"));
            assertThat(e.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasItem(XPackPlugin.MACHINE_LEARNING));
        }

        // Pick a license that does allow machine learning
        mode = randomValidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(true);
        // test that license restricted apis do now work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PutDatafeedAction.Response> listener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).putDatafeed(
                    new PutDatafeedAction.Request(createDatafeed("foobar", "foo", Collections.singletonList("foo"))), listener);
            PutDatafeedAction.Response response = listener.actionGet();
            assertNotNull(response);
        }
    }

    public void testMachineLearningStartDatafeedActionRestricted() throws Exception {

        assertMLAllowed(true);
        // test that license restricted apis do now work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PutJobAction.Response> putJobListener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).putJob(new PutJobAction.Request(createJob("foo").build(true, "foo")), putJobListener);
            PutJobAction.Response putJobResponse = putJobListener.actionGet();
            assertNotNull(putJobResponse);
            PlainListenableActionFuture<PutDatafeedAction.Response> putDatafeedListener = new PlainListenableActionFuture<>(
                    client.threadPool());
            new MachineLearningClient(client).putDatafeed(
                    new PutDatafeedAction.Request(createDatafeed("foobar", "foo", Collections.singletonList("foo"))), putDatafeedListener);
            PutDatafeedAction.Response putDatafeedResponse = putDatafeedListener.actionGet();
            assertNotNull(putDatafeedResponse);
            PlainListenableActionFuture<PersistentActionResponse> openJobListener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).openJob(new OpenJobAction.Request("foo"), openJobListener);
            PersistentActionResponse openJobResponse = openJobListener.actionGet();
            assertNotNull(openJobResponse);
        }

        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);
        // test that license restricted apis do not work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PersistentActionResponse> listener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).startDatafeed(new StartDatafeedAction.Request("foobar", 0L), listener);
            listener.actionGet();
            fail("start datafeed action should not be enabled!");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
            assertThat(e.getMessage(), containsString("non-compliant"));
            assertThat(e.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasItem(XPackPlugin.MACHINE_LEARNING));
        }

        // Pick a license that does allow machine learning
        mode = randomValidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(true);
        // test that license restricted apis do now work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PersistentActionResponse> listener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).startDatafeed(new StartDatafeedAction.Request("foobar", 0L), listener);
            PersistentActionResponse response = listener.actionGet();
            assertNotNull(response);
        }
    }

    public void testMachineLearningStopDatafeedActionNotRestricted() throws Exception {

        assertMLAllowed(true);
        // test that license restricted apis do now work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PutJobAction.Response> putJobListener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).putJob(new PutJobAction.Request(createJob("foo").build(true, "foo")), putJobListener);
            PutJobAction.Response putJobResponse = putJobListener.actionGet();
            assertNotNull(putJobResponse);
            PlainListenableActionFuture<PutDatafeedAction.Response> putDatafeedListener = new PlainListenableActionFuture<>(
                    client.threadPool());
            new MachineLearningClient(client).putDatafeed(
                    new PutDatafeedAction.Request(createDatafeed("foobar", "foo", Collections.singletonList("foo"))), putDatafeedListener);
            PutDatafeedAction.Response putDatafeedResponse = putDatafeedListener.actionGet();
            assertNotNull(putDatafeedResponse);
            PlainListenableActionFuture<PersistentActionResponse> openJobListener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).openJob(new OpenJobAction.Request("foo"), openJobListener);
            PersistentActionResponse openJobResponse = openJobListener.actionGet();
            assertNotNull(openJobResponse);
            PlainListenableActionFuture<PersistentActionResponse> startDatafeedListener = new PlainListenableActionFuture<>(
                    client.threadPool());
            new MachineLearningClient(client).startDatafeed(new StartDatafeedAction.Request("foobar", 0L), startDatafeedListener);
            PersistentActionResponse startDatafeedResponse = startDatafeedListener.actionGet();
            assertNotNull(startDatafeedResponse);
        }

        // Pick a random license
        License.OperationMode mode = randomLicenseType();
        enableLicensing(mode);

        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<RemovePersistentTaskAction.Response> listener = new PlainListenableActionFuture<>(
                    client.threadPool());
            new MachineLearningClient(client).stopDatafeed(new StopDatafeedAction.Request("foobar"), listener);
            listener.actionGet();
        }
    }

    public void testMachineLearningCloseJobActionNotRestricted() throws Exception {

        assertMLAllowed(true);
        // test that license restricted apis do now work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PutJobAction.Response> putJobListener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).putJob(new PutJobAction.Request(createJob("foo").build(true, "foo")), putJobListener);
            PutJobAction.Response putJobResponse = putJobListener.actionGet();
            assertNotNull(putJobResponse);
            PlainListenableActionFuture<PersistentActionResponse> openJobListener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).openJob(new OpenJobAction.Request("foo"), openJobListener);
            PersistentActionResponse openJobResponse = openJobListener.actionGet();
            assertNotNull(openJobResponse);
        }

        // Pick a random license
        License.OperationMode mode = randomLicenseType();
        enableLicensing(mode);

        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<CloseJobAction.Response> listener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).closeJob(new CloseJobAction.Request("foo"), listener);
            listener.actionGet();
        }
    }

    public void testMachineLearningDeleteJobActionNotRestricted() throws Exception {

        assertMLAllowed(true);
        // test that license restricted apis do now work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PutJobAction.Response> putJobListener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).putJob(new PutJobAction.Request(createJob("foo").build(true, "foo")), putJobListener);
            PutJobAction.Response putJobResponse = putJobListener.actionGet();
            assertNotNull(putJobResponse);
        }

        // Pick a random license
        License.OperationMode mode = randomLicenseType();
        enableLicensing(mode);

        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<DeleteJobAction.Response> listener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).deleteJob(new DeleteJobAction.Request("foo"), listener);
            listener.actionGet();
        }
    }

    public void testMachineLearningDeleteDatafeedActionNotRestricted() throws Exception {

        assertMLAllowed(true);
        // test that license restricted apis do now work
        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<PutJobAction.Response> putJobListener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).putJob(new PutJobAction.Request(createJob("foo").build(true, "foo")), putJobListener);
            PutJobAction.Response putJobResponse = putJobListener.actionGet();
            assertNotNull(putJobResponse);
            PlainListenableActionFuture<PutDatafeedAction.Response> putDatafeedListener = new PlainListenableActionFuture<>(
                    client.threadPool());
            new MachineLearningClient(client).putDatafeed(
                    new PutDatafeedAction.Request(createDatafeed("foobar", "foo", Collections.singletonList("foo"))), putDatafeedListener);
            PutDatafeedAction.Response putDatafeedResponse = putDatafeedListener.actionGet();
            assertNotNull(putDatafeedResponse);
        }

        // Pick a random license
        License.OperationMode mode = randomLicenseType();
        enableLicensing(mode);

        try (TransportClient client = new TestXPackTransportClient(internalCluster().transportClient().settings())) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            PlainListenableActionFuture<DeleteDatafeedAction.Response> listener = new PlainListenableActionFuture<>(client.threadPool());
            new MachineLearningClient(client).deleteDatafeed(new DeleteDatafeedAction.Request("foobar"), listener);
            listener.actionGet();
        }
    }

    private static OperationMode randomInvalidLicenseType() {
        return randomFrom(License.OperationMode.GOLD, License.OperationMode.STANDARD, License.OperationMode.BASIC);
    }

    private static OperationMode randomValidLicenseType() {
        return randomFrom(License.OperationMode.TRIAL, License.OperationMode.PLATINUM);
    }

    private static OperationMode randomLicenseType() {
        return randomFrom(License.OperationMode.values());
    }

    private static void assertMLAllowed(boolean expected) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            assertEquals(licenseState.isMachineLearningAllowed(), expected);
        }
    }

    public static void disableLicensing() {
        disableLicensing(randomValidLicenseType());
    }

    public static void disableLicensing(License.OperationMode operationMode) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            licenseState.update(operationMode, false);
        }
    }

    public static void enableLicensing() {
        enableLicensing(randomValidLicenseType());
    }

    public static void enableLicensing(License.OperationMode operationMode) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            licenseState.update(operationMode, true);
        }
    }
}
