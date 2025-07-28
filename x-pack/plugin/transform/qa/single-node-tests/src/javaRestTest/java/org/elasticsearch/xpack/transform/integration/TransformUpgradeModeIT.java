/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TransformUpgradeModeIT extends TransformRestTestCase {
    private static boolean indicesCreated = false;

    // preserve indices in order to reuse source indices in several test cases
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void createIndexes() throws IOException {
        setupUser(TEST_USER_NAME, singletonList("transform_user"));
        setupUser(TEST_ADMIN_USER_NAME, singletonList("transform_admin"));
        // it's not possible to run it as @BeforeClass as clients aren't initialized then, so we need this little hack
        if (indicesCreated) {
            return;
        }

        createReviewsIndex();
        indicesCreated = true;
    }

    @After
    public void clearOutTransforms() throws Exception {
        adminClient().performRequest(new Request("POST", "/_features/_reset"));
    }

    public void testUpgradeMode() throws Exception {
        var transformId = startTransform();

        assertAcknowledged(setUpgradeMode(BASIC_AUTH_VALUE_TRANSFORM_ADMIN, true));

        var statsAndState = getTransformStateAndStats(transformId);
        assertThat(readString(statsAndState, "state"), equalTo(TransformStats.State.WAITING.value()));
        assertThat(readString(statsAndState, "reason"), equalTo("Transform task will not be assigned while upgrade mode is enabled."));

        assertAcknowledged(setUpgradeMode(BASIC_AUTH_VALUE_TRANSFORM_ADMIN, false));

        // upgrade mode only waits for the assignment block to be removed, not for the transform to restart
        // so we need to wait until the transform restarts
        assertBusy(() -> {
            var nextStatsAndState = getTransformStateAndStats(transformId);
            assertThat(readString(nextStatsAndState, "state"), equalTo(TransformStats.State.STARTED.value()));
            assertThat(readString(nextStatsAndState, "reason"), nullValue());
        }, 30, TimeUnit.SECONDS);
    }

    private String startTransform() throws Exception {
        var transformId = "pivot_continuous";
        createContinuousPivotReviewsTransform(transformId, "pivot_reviews_continuous", null);
        startAndWaitForContinuousTransform(transformId, "pivot_reviews_continuous", null);
        return transformId;
    }

    private Response setUpgradeMode(String auth, boolean enabled) throws Exception {
        var path = enabled ? "set_upgrade_mode?enabled" : "set_upgrade_mode";
        var setUpgradeMode = createRequestWithAuth("POST", getTransformEndpoint() + path, auth);
        return client().performRequest(setUpgradeMode);
    }

    private String readString(Map<?, ?> statsAndState, String key) {
        return statsAndState == null ? null : (String) XContentMapValues.extractValue(key, statsAndState);
    }

    public void testUpgradeModeFailsWithNonAdminUser() throws Exception {
        assertRestException(
            () -> setUpgradeMode(TEST_USER_NAME, true),
            e -> assertThat(
                "Expected security error using non-admin transform user",
                e.getResponse().getStatusLine().getStatusCode(),
                is(401)
            )
        );
    }

    @SuppressWarnings("unchecked")
    public void testUpgradeModeBlocksRestApi() throws Exception {
        var stoppedTransform = "stopped-transform";
        createContinuousPivotReviewsTransform(stoppedTransform, "pivot_reviews_continuous", null);

        var startedTransform = startTransform();

        var uncreatedTransform = "some-new-transform";

        assertAcknowledged(setUpgradeMode(BASIC_AUTH_VALUE_TRANSFORM_ADMIN, true));

        // start an existing transform
        assertConflict(() -> startTransform(stoppedTransform), "Cannot start any Transform while the Transform feature is upgrading.");

        // stop an existing transform
        assertConflict(() -> stopTransform(startedTransform, false), "Cannot stop any Transform while the Transform feature is upgrading.");

        // create a new transform
        assertConflict(
            () -> createContinuousPivotReviewsTransform(uncreatedTransform, "pivot_reviews_continuous", null),
            "Cannot create new Transform while the Transform feature is upgrading."
        );

        // update an existing transform
        assertConflict(
            () -> updateTransform(stoppedTransform, "{ \"settings\": { \"max_page_search_size\": 123 } }", false),
            "Cannot update any Transform while the Transform feature is upgrading."
        );

        // upgrade all transforms
        assertConflict(this::upgrade, "Cannot upgrade Transforms while the Transform feature is upgrading.");

        // schedule transform
        assertConflict(
            () -> scheduleNowTransform(startedTransform),
            "Cannot schedule any Transform while the Transform feature is upgrading."
        );

        // reset transform
        assertConflict(
            () -> resetTransform(startedTransform, false),
            "Cannot reset any Transform while the Transform feature is upgrading."
        );

        assertAcknowledged(setUpgradeMode(BASIC_AUTH_VALUE_TRANSFORM_ADMIN, false));

        // started transform should go back into started
        assertBusy(() -> {
            var statsAndState = getTransformStateAndStats(startedTransform);
            assertThat(readString(statsAndState, "state"), equalTo(TransformStats.State.STARTED.value()));
            assertThat(readString(statsAndState, "reason"), nullValue());
        }, 30, TimeUnit.SECONDS);

        // stopped transform should have never started
        var statsAndState = getTransformStateAndStats(stoppedTransform);
        assertThat(readString(statsAndState, "state"), equalTo(TransformStats.State.STOPPED.value()));
        assertThat(readString(statsAndState, "reason"), nullValue());

        // new transform shouldn't exist
        assertRestException(() -> getTransformConfig(uncreatedTransform, BASIC_AUTH_VALUE_TRANSFORM_ADMIN), e -> {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(404));
            assertThat(e.getMessage(), containsString("Transform with id [" + uncreatedTransform + "] could not be found"));
        });

        // stopped transform should not have been updated
        var stoppedConfig = getTransformConfig(stoppedTransform, BASIC_AUTH_VALUE_TRANSFORM_ADMIN);
        assertThat((Map<String, Object>) stoppedConfig.get("settings"), is(anEmptyMap()));
    }

    private void assertConflict(CheckedRunnable<Exception> runnable, String message) throws Exception {
        assertRestException(runnable, e -> {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(409));
            assertThat(e.getMessage(), containsString(message));
        });
    }

    private void assertRestException(CheckedRunnable<Exception> runnable, Consumer<ResponseException> assertThat) throws Exception {
        try {
            runnable.run();
            fail("Expected code to throw ResponseException.");
        } catch (ResponseException e) {
            assertThat.accept(e);
        }
    }

    private void upgrade() throws IOException {
        client().performRequest(
            createRequestWithAuth("POST", TransformField.REST_BASE_PATH_TRANSFORMS + "_upgrade", BASIC_AUTH_VALUE_TRANSFORM_ADMIN)
        );
    }
}
