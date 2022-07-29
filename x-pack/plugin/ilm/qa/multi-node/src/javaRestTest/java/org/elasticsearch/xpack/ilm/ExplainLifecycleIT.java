/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.junit.Before;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createFullPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createIndexWithSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explain;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ExplainLifecycleIT extends ESRestTestCase {
    private static final Logger logger = LogManager.getLogger(ExplainLifecycleIT.class);
    private static final String FAILED_STEP_RETRY_COUNT_FIELD = "failed_step_retry_count";
    private static final String IS_AUTO_RETRYABLE_ERROR_FIELD = "is_auto_retryable_error";

    private String policy;
    private String index;
    private String alias;

    @Before
    public void refreshIndex() {
        index = "index-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        alias = "alias-" + randomAlphaOfLength(5);
    }

    public void testExplainFilters() throws Exception {
        String goodIndex = index + "-good-000001";
        String errorIndex = index + "-error";
        String nonexistantPolicyIndex = index + "-nonexistant-policy";
        String unmanagedIndex = index + "-unmanaged";

        createFullPolicy(client(), policy, TimeValue.ZERO);

        {
            // Create a "shrink-only-policy"
            Map<String, LifecycleAction> warmActions = new HashMap<>();
            warmActions.put(ShrinkAction.NAME, new ShrinkAction(17, null));
            Map<String, Phase> phases = new HashMap<>();
            phases.put("warm", new Phase("warm", TimeValue.ZERO, warmActions));
            LifecyclePolicy lifecyclePolicy = new LifecyclePolicy("shrink-only-policy", phases);
            // PUT policy
            XContentBuilder builder = jsonBuilder();
            lifecyclePolicy.toXContent(builder, null);
            final StringEntity entity = new StringEntity("{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
            Request request = new Request("PUT", "_ilm/policy/shrink-only-policy");
            request.setEntity(entity);
            assertOK(client().performRequest(request));
        }

        createIndexWithSettings(
            client(),
            goodIndex,
            alias,
            Settings.builder()
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
        );
        createIndexWithSettings(
            client(),
            errorIndex,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(LifecycleSettings.LIFECYCLE_NAME, "shrink-only-policy")
        );
        createIndexWithSettings(
            client(),
            nonexistantPolicyIndex,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, randomValueOtherThan(policy, () -> randomAlphaOfLengthBetween(3, 10)))
        );
        createIndexWithSettings(client(), unmanagedIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));

        assertBusy(() -> {
            Map<String, Map<String, Object>> explainResponse = explain(client(), index + "*", false, false);
            assertNotNull(explainResponse);
            assertThat(
                explainResponse,
                allOf(hasKey(goodIndex), hasKey(errorIndex), hasKey(nonexistantPolicyIndex), hasKey(unmanagedIndex))
            );

            Map<String, Map<String, Object>> onlyManagedResponse = explain(client(), index + "*", false, true);
            assertNotNull(onlyManagedResponse);
            assertThat(onlyManagedResponse, allOf(hasKey(goodIndex), hasKey(errorIndex), hasKey(nonexistantPolicyIndex)));
            assertThat(onlyManagedResponse, not(hasKey(unmanagedIndex)));

            Map<String, Map<String, Object>> onlyErrorsResponse = explain(client(), index + "*", true, true);
            assertNotNull(onlyErrorsResponse);
            assertThat(onlyErrorsResponse, hasKey(nonexistantPolicyIndex));
            assertThat(onlyErrorsResponse, allOf(not(hasKey(goodIndex)), not(hasKey(unmanagedIndex))));
        });
    }

    public void testExplainIndexContainsAutomaticRetriesInformation() throws Exception {
        createFullPolicy(client(), policy, TimeValue.ZERO);

        // create index without alias so the rollover action fails and is retried
        createIndexWithSettings(
            client(),
            index,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(LifecycleSettings.LIFECYCLE_NAME, policy)
        );

        assertBusy(() -> {
            Map<String, Object> explainIndex = explainIndex(client(), index);
            assertThat((Integer) explainIndex.get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1));
            assertThat(explainIndex.get(IS_AUTO_RETRYABLE_ERROR_FIELD), is(true));
        });
    }

    @SuppressWarnings("unchecked")
    public void testExplainIndicesWildcard() throws Exception {
        createNewSingletonPolicy(client(), policy, "delete", DeleteAction.WITH_SNAPSHOT_DELETE, TimeValue.timeValueDays(100));
        String firstIndex = this.index + "-first";
        String secondIndex = this.index + "-second";
        String unmanagedIndex = this.index + "-unmanaged";
        String indexWithMissingPolicy = this.index + "-missing_policy";
        createIndexWithSettings(
            client(),
            firstIndex,
            alias + firstIndex,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
        );
        createIndexWithSettings(
            client(),
            secondIndex,
            alias + secondIndex,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
        );
        createIndexWithSettings(
            client(),
            unmanagedIndex,
            alias + unmanagedIndex,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        );
        String missingPolicyName = "missing_policy_";
        createIndexWithSettings(
            client(),
            indexWithMissingPolicy,
            alias + indexWithMissingPolicy,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, missingPolicyName)
        );

        assertBusy(() -> {
            Map<String, Map<String, Object>> explain = explain(client(), this.index + "*", false, false);
            assertManagedIndex(explain.get(firstIndex));
            assertManagedIndex(explain.get(secondIndex));
            assertUnmanagedIndex(explain.get(unmanagedIndex));

            Map<String, Object> explainIndexWithMissingPolicy = explain.get(indexWithMissingPolicy);
            assertThat(explainIndexWithMissingPolicy.get("managed"), is(true));
            assertThat(explainIndexWithMissingPolicy.get("policy"), is(missingPolicyName));
            assertThat(explainIndexWithMissingPolicy.get("phase"), is(nullValue()));
            assertThat(explainIndexWithMissingPolicy.get("action"), is(nullValue()));
            assertThat(explainIndexWithMissingPolicy.get("step"), is(nullValue()));
            assertThat(explainIndexWithMissingPolicy.get("age"), is(nullValue()));
            assertThat(explainIndexWithMissingPolicy.get("failed_step"), is(nullValue()));
            Map<String, Object> stepInfo = (Map<String, Object>) explainIndexWithMissingPolicy.get("step_info");
            assertThat(stepInfo, is(notNullValue()));
            assertThat(stepInfo.get("reason"), is("policy [missing_policy_] does not exist"));
        });
    }

    private void assertUnmanagedIndex(Map<String, Object> explainIndexMap) {
        assertThat(explainIndexMap.get("managed"), is(false));
        assertThat(explainIndexMap.get("time_since_index_creation"), is(nullValue()));
        assertThat(explainIndexMap.get("index_creation_date_millis"), is(nullValue()));
        assertThat(explainIndexMap.get("policy"), is(nullValue()));
        assertThat(explainIndexMap.get("phase"), is(nullValue()));
        assertThat(explainIndexMap.get("action"), is(nullValue()));
        assertThat(explainIndexMap.get("step"), is(nullValue()));
        assertThat(explainIndexMap.get("age"), is(nullValue()));
        assertThat(explainIndexMap.get("failed_step"), is(nullValue()));
        assertThat(explainIndexMap.get("step_info"), is(nullValue()));
    }

    private void assertManagedIndex(Map<String, Object> explainIndexMap) {
        assertThat(explainIndexMap.get("managed"), is(true));
        assertThat(explainIndexMap.get("time_since_index_creation"), is(notNullValue()));
        assertThat(explainIndexMap.get("index_creation_date_millis"), is(notNullValue()));
        assertThat(explainIndexMap.get("policy"), is(policy));
        assertThat(explainIndexMap.get("phase"), is("new"));
        assertThat(explainIndexMap.get("action"), is("complete"));
        assertThat(explainIndexMap.get("step"), is("complete"));
        assertThat(explainIndexMap.get("phase_time_millis"), is(notNullValue()));
        assertThat(explainIndexMap.get("age"), is(notNullValue()));
        assertThat(explainIndexMap.get("phase_execution"), is(notNullValue()));
        assertThat(explainIndexMap.get("failed_step"), is(nullValue()));
        assertThat(explainIndexMap.get("step_info"), is(nullValue()));
    }

}
