/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.junit.Before;

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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class LifecycleLicenseIT extends ESRestTestCase {

    private String policy;
    private String dataStream;

    @Before
    public void refreshDatastream() {
        dataStream = "logs-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
    }

    public void testCreatePolicyUsingActionAndNonCompliantLicense() throws Exception {
        String snapshotRepo = randomAlphaOfLengthBetween(4, 10);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        assertOK(client().performRequest(new Request("DELETE", "/_license")));

        ResponseException exception = expectThrows(ResponseException.class,
            () -> createNewSingletonPolicy(client(), policy, "cold", new SearchableSnapshotAction(snapshotRepo, true)));
        assertThat(EntityUtils.toString(exception.getResponse().getEntity()),
            containsStringIgnoringCase("policy [" + policy + "] defines the [" + SearchableSnapshotAction.NAME + "] action but the " +
                "current license is non-compliant for [searchable-snapshots]"));
    }

    public void testSearchableSnapshotActionErrorsOnInvalidLicense() throws Exception {
        String snapshotRepo = randomAlphaOfLengthBetween(4, 10);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createNewSingletonPolicy(client(), policy, "cold", new SearchableSnapshotAction(snapshotRepo, true));

        createComposableTemplate(client(), "template-name", dataStream,
            new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(), null, null));

        assertOK(client().performRequest(new Request("DELETE", "/_license")));

        indexDocument(client(), dataStream, true);

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        rolloverMaxOneDocCondition(client(), dataStream);

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1L);
        // the shrink action should start failing (and retrying) due to invalid license
        assertBusy(() -> {
            Map<String, Object> explainIndex = explainIndex(client(), backingIndexName);
            assertThat(explainIndex.get("action"), is(SearchableSnapshotAction.NAME));
            assertThat((Integer) explainIndex.get("failed_step_retry_count"), greaterThanOrEqualTo(1));

        }, 30, TimeUnit.SECONDS);
    }
}
