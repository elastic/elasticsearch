/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.documentation;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.license.StartTrialRequest;
import org.elasticsearch.client.license.StartTrialResponse;
import org.elasticsearch.client.license.StartBasicRequest;
import org.elasticsearch.client.license.StartBasicResponse;
import org.elasticsearch.client.license.GetBasicStatusResponse;
import org.elasticsearch.client.license.GetTrialStatusResponse;
import org.elasticsearch.core.Booleans;
import org.junit.After;
import org.junit.BeforeClass;
import org.elasticsearch.client.license.DeleteLicenseRequest;
import org.elasticsearch.client.license.GetLicenseRequest;
import org.elasticsearch.client.license.GetLicenseResponse;
import org.elasticsearch.client.license.LicensesStatus;
import org.elasticsearch.client.license.PutLicenseRequest;
import org.elasticsearch.client.license.PutLicenseResponse;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.LicenseIT.putTrialLicense;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

/**
 * Documentation for Licensing APIs in the high level java client.
 * Code wrapped in {@code tag} and {@code end} tags is included in the docs.
 */
public class LicensingDocumentationIT extends ESRestHighLevelClientTestCase {

    @BeforeClass
    public static void checkForSnapshot() {
        assumeTrue("Trial license used to rollback is only valid when tested against snapshot/test builds",
            Build.CURRENT.isSnapshot());
    }

    @After
    public void rollbackToTrial() throws IOException {
        putTrialLicense();
    }

    public void testLicense() throws Exception {
        RestHighLevelClient client = highLevelClient();
        String license = "{\"license\": {\"uid\":\"893361dc-9749-4997-93cb-802e3d7fa4a8\",\"type\":\"gold\"," +
            "\"issue_date_in_millis\":1411948800000,\"expiry_date_in_millis\":1914278399999,\"max_nodes\":1,\"issued_to\":\"issued_to\"," +
            "\"issuer\":\"issuer\",\"signature\":\"AAAAAgAAAA3U8+YmnvwC+CWsV/mRAAABmC9ZN0hjZDBGYnVyRXpCOW5Bb3FjZDAxOWpSbTVoMVZwUzRxVk1PSm" +
            "kxakxZdW5IMlhlTHNoN1N2MXMvRFk4d3JTZEx3R3RRZ0pzU3lobWJKZnQvSEFva0ppTHBkWkprZWZSQi9iNmRQNkw1SlpLN0lDalZCS095MXRGN1lIZlpYcVVTTn" +
            "FrcTE2dzhJZmZrdFQrN3JQeGwxb0U0MXZ0dDJHSERiZTVLOHNzSDByWnpoZEphZHBEZjUrTVBxRENNSXNsWWJjZllaODdzVmEzUjNiWktNWGM5TUhQV2plaUo4Q1" +
            "JOUml4MXNuL0pSOEhQaVB2azhmUk9QVzhFeTFoM1Q0RnJXSG53MWk2K055c28zSmRnVkF1b2JSQkFLV2VXUmVHNDZ2R3o2VE1qbVNQS2lxOHN5bUErZlNIWkZSVm" +
            "ZIWEtaSU9wTTJENDVvT1NCYklacUYyK2FwRW9xa0t6dldMbmMzSGtQc3FWOTgzZ3ZUcXMvQkt2RUZwMFJnZzlvL2d2bDRWUzh6UG5pdENGWFRreXNKNkE9PQAAAQ" +
            "Be8GfzDm6T537Iuuvjetb3xK5dvg0K5NQapv+rczWcQFxgCuzbF8plkgetP1aAGZP4uRESDQPMlOCsx4d0UqqAm9f7GbBQ3l93P+PogInPFeEH9NvOmaAQovmxVM" +
            "9SE6DsDqlX4cXSO+bgWpXPTd2LmpoQc1fXd6BZ8GeuyYpVHVKp9hVU0tAYjw6HzYOE7+zuO1oJYOxElqy66AnIfkvHrvni+flym3tE7tDTgsDRaz7W3iBhaqiSnt" +
            "EqabEkvHdPHQdSR99XGaEvnHO1paK01/35iZF6OXHsF7CCj+558GRXiVxzueOe7TsGSSt8g7YjZwV9bRCyU7oB4B/nidgI\"}}";
        {
            //tag::put-license-execute
            PutLicenseRequest request = new PutLicenseRequest();
            request.setLicenseDefinition(license);  // <1>
            request.setAcknowledge(false);          // <2>

            PutLicenseResponse response = client.license().putLicense(request, RequestOptions.DEFAULT);
            //end::put-license-execute

            //tag::put-license-response
            LicensesStatus status = response.status();                  // <1>
            assertEquals(status, LicensesStatus.VALID);                 // <2>
            boolean acknowledged = response.isAcknowledged();           // <3>
            String acknowledgeHeader = response.acknowledgeHeader();    // <4>
            Map<String, String[]> acknowledgeMessages = response.acknowledgeMessages();  // <5>
            //end::put-license-response

            assertFalse(acknowledged); // Should fail because we are trying to downgrade from platinum trial to gold
            assertThat(acknowledgeHeader, startsWith("This license update requires acknowledgement."));
            assertThat(acknowledgeMessages.keySet(), not(hasSize(0)));
        }
        {
            PutLicenseRequest request = new PutLicenseRequest();
            // tag::put-license-execute-listener
            ActionListener<PutLicenseResponse> listener = new ActionListener<PutLicenseResponse>() {
                @Override
                public void onResponse(PutLicenseResponse putLicenseResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-license-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-license-execute-async
            client.license().putLicenseAsync(
                    request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-license-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        // we cannot actually delete the license, otherwise the remaining tests won't work
        if (Booleans.isTrue("true")) {
            return;
        }
        {
            //tag::delete-license-execute
            DeleteLicenseRequest request = new DeleteLicenseRequest();

            AcknowledgedResponse response = client.license().deleteLicense(request, RequestOptions.DEFAULT);
            //end::delete-license-execute

            //tag::delete-license-response
            boolean acknowledged = response.isAcknowledged(); // <1>
            //end::delete-license-response

            assertTrue(acknowledged);
        }
        {
            DeleteLicenseRequest request = new DeleteLicenseRequest();
            // tag::delete-license-execute-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse deleteLicenseResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-license-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-license-execute-async
            client.license().deleteLicenseAsync(
                request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-license-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetLicense() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::get-license-execute
            GetLicenseRequest request = new GetLicenseRequest();

            GetLicenseResponse response = client.license().getLicense(request, RequestOptions.DEFAULT);
            //end::get-license-execute

            //tag::get-license-response
            String currentLicense = response.getLicenseDefinition(); // <1>
            //end::get-license-response

            assertThat(currentLicense, containsString("trial"));
            assertThat(currentLicense, containsString("ntegTest"));
        }
        {
            GetLicenseRequest request = new GetLicenseRequest();
            // tag::get-license-execute-listener
            ActionListener<GetLicenseResponse> listener = new ActionListener<GetLicenseResponse>() {
                @Override
                public void onResponse(GetLicenseResponse indexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-license-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-license-execute-async
            client.license().getLicenseAsync(
                request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-license-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
        {
            GetLicenseRequest request = new GetLicenseRequest();
            RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
            // Make sure that it still works in other formats
            builder.addHeader("Accept", randomFrom("application/smile", "application/cbor"));
            RequestOptions options = builder.build();
            GetLicenseResponse response = client.license().getLicense(request, options);
            String currentLicense = response.getLicenseDefinition();
            assertThat(currentLicense, startsWith("{"));
            assertThat(currentLicense, containsString("trial"));
            assertThat(currentLicense, containsString("ntegTest"));
            assertThat(currentLicense, endsWith("}"));
        }
    }

    public void testStartTrial() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::start-trial-execute
            StartTrialRequest request = new StartTrialRequest(true); // <1>

            StartTrialResponse response = client.license().startTrial(request, RequestOptions.DEFAULT);
            // end::start-trial-execute

            // tag::start-trial-response
            boolean acknowledged = response.isAcknowledged();                              // <1>
            boolean trialWasStarted = response.isTrialWasStarted();                        // <2>
            String licenseType = response.getLicenseType();                                // <3>
            String errorMessage = response.getErrorMessage();                              // <4>
            String acknowledgeHeader = response.getAcknowledgeHeader();                    // <5>
            Map<String, String[]> acknowledgeMessages = response.getAcknowledgeMessages(); // <6>
            // end::start-trial-response

            assertTrue(acknowledged);
            assertFalse(trialWasStarted);
            assertThat(licenseType, nullValue());
            assertThat(errorMessage, is("Operation failed: Trial was already activated."));
            assertThat(acknowledgeHeader, nullValue());
            assertThat(acknowledgeMessages, nullValue());
        }

        {
            StartTrialRequest request = new StartTrialRequest();

            // tag::start-trial-execute-listener
            ActionListener<StartTrialResponse> listener = new ActionListener<StartTrialResponse>() {
                @Override
                public void onResponse(StartTrialResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::start-trial-execute-listener

            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::start-trial-execute-async
            client.license().startTrialAsync(request, RequestOptions.DEFAULT, listener);
            // end::start-trial-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPostStartBasic() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::start-basic-execute
            StartBasicRequest request = new StartBasicRequest();

            StartBasicResponse response = client.license().startBasic(request, RequestOptions.DEFAULT);
            //end::start-basic-execute

            //tag::start-basic-response
            boolean acknowledged = response.isAcknowledged();                              // <1>
            boolean basicStarted = response.isBasicStarted();                              // <2>
            String errorMessage = response.getErrorMessage();                              // <3>
            String acknowledgeMessage = response.getAcknowledgeMessage();                  // <4>
            Map<String, String[]> acknowledgeMessages = response.getAcknowledgeMessages(); // <5>
            //end::start-basic-response
        }
        {
            StartBasicRequest request = new StartBasicRequest();
            // tag::start-basic-listener
            ActionListener<StartBasicResponse> listener = new ActionListener<StartBasicResponse>() {
                @Override
                public void onResponse(StartBasicResponse indexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::start-basic-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::start-basic-execute-async
            client.license().startBasicAsync(
                request, RequestOptions.DEFAULT, listener); // <1>
            // end::start-basic-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetTrialStatus() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::get-trial-status-execute
            GetTrialStatusResponse response = client.license().getTrialStatus(RequestOptions.DEFAULT);
            //end::get-trial-status-execute

            //tag::get-trial-status-response
            boolean eligibleToStartTrial = response.isEligibleToStartTrial(); // <1>
            //end::get-trial-status-response
        }
    }

    public void testGetBasicStatus() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::get-basic-status-execute
            GetBasicStatusResponse response = client.license().getBasicStatus(RequestOptions.DEFAULT);
            //end::get-basic-status-execute

            //tag::get-basic-status-response
            boolean eligibleToStartbasic = response.isEligibleToStartBasic(); // <1>
            //end::get-basic-status-response
        }
    }
}
