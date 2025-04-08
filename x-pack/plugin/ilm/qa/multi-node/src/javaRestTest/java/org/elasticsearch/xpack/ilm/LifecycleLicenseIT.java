/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createComposableTemplate;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createSnapshotRepo;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.rolloverMaxOneDocCondition;
import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class LifecycleLicenseIT extends ESRestTestCase {

    private String policy;
    private String dataStream;

    @Before
    public void refreshDatastream() throws Exception {
        dataStream = "logs-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        waitForActiveLicense(adminClient());
    }

    @After
    public void resetLicenseToTrial() throws Exception {
        putTrialLicense();
        checkCurrentLicenseIs("trial");
    }

    public void testCreatePolicyUsingActionAndNonCompliantLicense() throws Exception {
        String snapshotRepo = randomAlphaOfLengthBetween(4, 10);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());

        assertOK(client().performRequest(new Request("DELETE", "/_license")));
        checkCurrentLicenseIs("basic");

        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> createNewSingletonPolicy(client(), policy, "cold", new SearchableSnapshotAction(snapshotRepo, true))
        );
        assertThat(
            EntityUtils.toString(exception.getResponse().getEntity()),
            containsStringIgnoringCase(
                "policy ["
                    + policy
                    + "] defines the ["
                    + SearchableSnapshotAction.NAME
                    + "] action but the "
                    + "current license is non-compliant for [searchable-snapshots]"
            )
        );
    }

    @SuppressWarnings("unchecked")
    public void testSearchableSnapshotActionErrorsOnInvalidLicense() throws Exception {
        String snapshotRepo = randomAlphaOfLengthBetween(4, 10);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createNewSingletonPolicy(client(), policy, "cold", new SearchableSnapshotAction(snapshotRepo, true));

        createComposableTemplate(
            client(),
            "template-name",
            dataStream,
            new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(), null, null)
        );

        assertOK(client().performRequest(new Request("DELETE", "/_license")));
        checkCurrentLicenseIs("basic");

        indexDocument(client(), dataStream, true);

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        rolloverMaxOneDocCondition(client(), dataStream);

        List<String> backingIndices = getDataStreamBackingIndexNames(dataStream);
        assertThat(backingIndices.size(), equalTo(2));
        String backingIndexName = backingIndices.getFirst();
        // the searchable_snapshot action should start failing (and retrying) due to invalid license
        assertBusy(() -> {
            Map<String, Object> explainIndex = explainIndex(client(), backingIndexName);
            assertThat(explainIndex.get("action"), is(SearchableSnapshotAction.NAME));
            assertThat((Integer) explainIndex.get("failed_step_retry_count"), greaterThanOrEqualTo(1));

            // this check is lenient to avoid test flakiness (when we retry steps we move ILM between the ERROR step and the
            // failed step - the `failed_step_retry_count` field is present in both steps until we move to the next step (ie.
            // until the failed step is executed successfully).
            // So, *if* we catch ILM in the ERROR step, we check the failed message
            if (ErrorStep.NAME.equals(explainIndex.get("step"))) {
                assertThat(
                    ((Map<String, String>) explainIndex.get("step_info")).get("reason"),
                    containsStringIgnoringCase("current license is non-compliant for [searchable-snapshots]")
                );
            }
        }, 30, TimeUnit.SECONDS);

        // switching back to trial so searchable_snapshot is permitted
        putTrialLicense();
        checkCurrentLicenseIs("trial");

        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + backingIndexName;
        assertTrue(waitUntil(() -> {
            try {
                return indexExists(restoredIndexName);
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));

        assertBusy(
            () -> assertThat(explainIndex(client(), restoredIndexName).get("step"), is(PhaseCompleteStep.NAME)),
            30,
            TimeUnit.SECONDS
        );
    }

    private void putTrialLicense() throws Exception {
        License signedLicense = TestUtils.generateSignedLicense("trial", License.VERSION_CURRENT, -1, TimeValue.timeValueDays(14));
        Request putTrialRequest = new Request("PUT", "/_license");
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder = signedLicense.toXContent(builder, ToXContent.EMPTY_PARAMS);
        putTrialRequest.setJsonEntity("{\"licenses\":[\n " + Strings.toString(builder) + "\n]}");
        assertBusy(() -> {
            Response putLicenseResponse = client().performRequest(putTrialRequest);
            logger.info("put trial license response body is [{}]", EntityUtils.toString(putLicenseResponse.getEntity()));
            assertOK(putLicenseResponse);
        });
    }

    private void checkCurrentLicenseIs(String type) throws Exception {
        assertBusy(() -> {
            Response getLicense = client().performRequest(new Request("GET", "/_license"));
            String responseBody = EntityUtils.toString(getLicense.getEntity());
            logger.info("get license response body is [{}]", responseBody);
            assertThat(responseBody, containsStringIgnoringCase("\"type\" : \"" + type + "\""));
        });
    }
}
