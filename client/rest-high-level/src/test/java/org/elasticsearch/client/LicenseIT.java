/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.elasticsearch.Build;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.license.DeleteLicenseRequest;
import org.elasticsearch.client.license.GetBasicStatusResponse;
import org.elasticsearch.client.license.GetLicenseRequest;
import org.elasticsearch.client.license.GetLicenseResponse;
import org.elasticsearch.client.license.GetTrialStatusResponse;
import org.elasticsearch.client.license.LicensesStatus;
import org.elasticsearch.client.license.PutLicenseRequest;
import org.elasticsearch.client.license.PutLicenseResponse;
import org.elasticsearch.client.license.StartBasicRequest;
import org.elasticsearch.client.license.StartBasicResponse;
import org.elasticsearch.client.license.StartTrialRequest;
import org.elasticsearch.client.license.StartTrialResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;

public class LicenseIT extends ESRestHighLevelClientTestCase {

    /*
     * todo there are some cases we can't test here because this gradle project starts the integ test cluster so that it'll generate
     * a trial license at startup. we need to add a separate gradle project for the license-related tests so that we can start the
     * integ test cluster without generating a trial license
     */

    @BeforeClass
    public static void checkForSnapshot() {
        assumeTrue("Trial license used to rollback is only valid when tested against snapshot/test builds",
            Build.CURRENT.isSnapshot());
    }

    @After
    public void rollbackToTrial() throws IOException {
        putTrialLicense();
    }

    public void testStartTrial() throws Exception {

        // todo add case where we successfully start a trial - see note above

        // case where we don't acknowledge trial license conditions
        {
            final StartTrialRequest request = new StartTrialRequest();
            final StartTrialResponse response = highLevelClient().license().startTrial(request, RequestOptions.DEFAULT);

            assertThat(response.isAcknowledged(), equalTo(false));
            assertThat(response.isTrialWasStarted(), equalTo(false));
            assertThat(response.getLicenseType(), nullValue());
            assertThat(response.getErrorMessage(), equalTo("Operation failed: Needs acknowledgement."));
            assertThat(response.getAcknowledgeHeader(), containsString("This API initiates a free 30-day trial for all platinum features"));
            assertNotEmptyAcknowledgeMessages(response.getAcknowledgeMessages());
        }

        // case where we acknowledge, but the trial is already started at cluster startup
        {
            final StartTrialRequest request = new StartTrialRequest(true);
            final StartTrialResponse response = highLevelClient().license().startTrial(request, RequestOptions.DEFAULT);

            assertThat(response.isAcknowledged(), equalTo(true));
            assertThat(response.isTrialWasStarted(), equalTo(false));
            assertThat(response.getLicenseType(), nullValue());
            assertThat(response.getErrorMessage(), equalTo("Operation failed: Trial was already activated."));
            assertThat(response.getAcknowledgeHeader(), nullValue());
            assertThat(response.getAcknowledgeMessages(), nullValue());
        }
    }

    public static void putTrialLicense() throws IOException {
        assumeTrue("Trial license is only valid when tested against snapshot/test builds",
            Build.CURRENT.isSnapshot());

        // use a hard-coded trial license for 20 yrs to be able to roll back from another licenses
        final String signature =
                "AAAABAAAAA3FXON9kGmNqmH+ASDWAAAAIAo5/x6hrsGh1GqqrJmy4qgmEC7gK0U4zQ6q5ZEMhm4jAAABAAcdKHL0BfM2uqTgT7BDuFxX5lb"
                        + "t/bHDVJ421Wwgm5p3IMbw/W13iiAHz0hhDziF7acJbc/y65L+BKGtVC1gSSHeLDHaAD66VrjKxfc7VbGyJIAYBOdujf0rheurmaD3IcNo"
                        + "/tWDjCdtTwrNziFkorsGcPadBP5Yc6csk3/Q74DlfiYweMBxLUfkBERwxwd5OQS6ujGvl/4bb8p5zXvOw8vMSaAXSXXnExP6lam+0934W"
                        + "0kHvU7IGk+fCUjOaiSWKSoE4TEcAtVNYj/oRoRtfQ1KQGpdCHxTHs1BimdZaG0nBHDsvhYlVVLSvHN6QzqsHWgFDG6JJxhtU872oTRSUHA=";
        final String licenseDefinition = Strings.toString(jsonBuilder()
            .startObject()
            .field("licenses", List.of(
                Map.of(
                    "uid", "96fc37c6-6fc9-43e2-a40d-73143850cd72",
                    "type", "trial",
                    // 2018-10-16 07:02:48 UTC
                    "issue_date_in_millis", "1539673368158",
                    // 2038-10-11 07:02:48 UTC, 20 yrs later
                    "expiry_date_in_millis", "2170393368158",
                    "max_nodes", "5",
                    "issued_to", "client_rest-high-level_integTestCluster",
                    "issuer", "elasticsearch",
                    "start_date_in_millis", "-1",
                    "signature", signature)))
            .endObject());

        final PutLicenseRequest request = new PutLicenseRequest();
        request.setAcknowledge(true);
        request.setLicenseDefinition(licenseDefinition);
        final PutLicenseResponse response = highLevelClient().license().putLicense(request, RequestOptions.DEFAULT);
        assertThat(response.isAcknowledged(), equalTo(true));
        assertThat(response.status(), equalTo(LicensesStatus.VALID));
    }

    public void testStartBasic() throws Exception {
        // we don't test the case where we successfully start a basic because the integ test cluster generates one on startup
        // and we don't have a good way to prevent that / work around it in this test project
        // case where we don't acknowledge basic license conditions
        {
            final StartBasicRequest request = new StartBasicRequest();
            final StartBasicResponse response = highLevelClient().license().startBasic(request, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), equalTo(false));
            assertThat(response.isBasicStarted(), equalTo(false));
            assertThat(response.getErrorMessage(), equalTo("Operation failed: Needs acknowledgement."));
            assertThat(response.getAcknowledgeMessage(),
                containsString("This license update requires acknowledgement. " +
                    "To acknowledge the license, please read the following messages and call /start_basic again"));
            assertNotEmptyAcknowledgeMessages(response.getAcknowledgeMessages());
        }
        // case where we acknowledge and the basic is started successfully
        {
            final StartBasicRequest request = new StartBasicRequest(true);
            final StartBasicResponse response = highLevelClient().license().startBasic(request, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), equalTo(true));
            assertThat(response.isBasicStarted(), equalTo(true));
            assertThat(response.getErrorMessage(), nullValue());
            assertThat(response.getAcknowledgeMessage(), nullValue());
            assertThat(response.getAcknowledgeMessages().size(), equalTo(0));
        }
    }

    private static void assertNotEmptyAcknowledgeMessages(Map<String, String[]> acknowledgeMessages) {
        assertThat(acknowledgeMessages.entrySet(), not(empty()));
        for (Map.Entry<String, String[]> entry : acknowledgeMessages.entrySet()) {
            assertThat(entry.getKey(), not(isEmptyOrNullString()));
            for (String message : entry.getValue()) {
                assertThat(message, not(isEmptyOrNullString()));
            }
        }
    }

    public void testGetLicense() throws Exception {
        final GetLicenseRequest request = new GetLicenseRequest();
        final GetLicenseResponse response = highLevelClient().license().getLicense(request, RequestOptions.DEFAULT);
        final String licenseDefinition = response.getLicenseDefinition();
        assertThat(licenseDefinition, notNullValue());

        final XContentParser parser = createParser(JsonXContent.jsonXContent, licenseDefinition);
        final Map<String, Object> map = parser.map();
        assertThat(map.containsKey("license"), equalTo(true));

        @SuppressWarnings("unchecked")
        final Map<String, Object> license = (Map<String, Object>) map.get("license");
        assertThat(license.get("status"), equalTo("active"));
        assertThat(license.get("type"), equalTo("trial"));
    }

    public void testPutLicense() throws Exception {
        putTrialLicense();
    }

    public void testDeleteLicense() throws Exception {
        final DeleteLicenseRequest request = new DeleteLicenseRequest();
        final AcknowledgedResponse response = highLevelClient().license().deleteLicense(request, RequestOptions.DEFAULT);
        assertThat(response.isAcknowledged(), equalTo(true));
    }

    public void testGetTrialStatus() throws IOException {
        GetTrialStatusResponse trialStatus = highLevelClient().license().getTrialStatus(RequestOptions.DEFAULT);
        assertFalse(trialStatus.isEligibleToStartTrial());
    }

    public void testGetBasicStatus() throws IOException {
        GetBasicStatusResponse basicStatus = highLevelClient().license().getBasicStatus(RequestOptions.DEFAULT);
        assertTrue(basicStatus.isEligibleToStartBasic());
    }
}
