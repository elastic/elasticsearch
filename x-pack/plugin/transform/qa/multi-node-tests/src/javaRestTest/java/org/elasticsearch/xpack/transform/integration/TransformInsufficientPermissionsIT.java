/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class TransformInsufficientPermissionsIT extends TransformRestTestCase {

    private static final String TEST_ADMIN_USERNAME = "x_pack_rest_user";
    private static final String TEST_ADMIN_HEADER = basicAuthHeaderValue(TEST_ADMIN_USERNAME, TEST_PASSWORD_SECURE_STRING);
    private static final String JUNIOR_USERNAME = "john_junior";
    private static final String JUNIOR_HEADER = basicAuthHeaderValue(JUNIOR_USERNAME, TEST_PASSWORD_SECURE_STRING);
    private static final String SENIOR_USERNAME = "bill_senior";
    private static final String SENIOR_HEADER = basicAuthHeaderValue(SENIOR_USERNAME, TEST_PASSWORD_SECURE_STRING);
    private static final String NOT_A_TRANSFORM_ADMIN = "not_a_transform_admin";
    private static final String NOT_A_TRANSFORM_ADMIN_HEADER = basicAuthHeaderValue(NOT_A_TRANSFORM_ADMIN, TEST_PASSWORD_SECURE_STRING);

    private static final int NUM_USERS = 28;

    // Transform Health statuses
    private static final String GREEN = "green";
    private static final String YELLOW = "yellow";
    private static final String RED = "red";

    @After
    public void cleanTransforms() throws Exception {
        cleanUp();
    }

    /**
     * defer_validation        = false
     * unattended              = false
     * pre-existing dest index = false
     */
    public void testTransformPermissionsNoDeferNoUnattended() throws Exception {
        testTransformPermissionsNoDefer(false);
    }

    /**
     * defer_validation        = false
     * unattended              = true
     * pre-existing dest index = false
     */
    public void testTransformPermissionsNoDeferUnattended() throws Exception {
        testTransformPermissionsNoDefer(true);
    }

    private void testTransformPermissionsNoDefer(boolean unattended) throws Exception {
        String transformId = "transform-permissions-nodefer-" + (unattended ? 1 : 0);
        String sourceIndexName = transformId + "-index";
        String destIndexName = sourceIndexName + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, unattended);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> putTransform(
                transformId,
                Strings.toString(config),
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader(AUTH_KEY, JUNIOR_HEADER)
                    .addParameter("defer_validation", String.valueOf(false))
                    .build()
            )
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(
            e.getMessage(),
            containsString(
                Strings.format(
                    "Cannot create transform [%s] because user %s lacks the required permissions "
                        + "[%s:[read, view_index_metadata], %s:[create_index, index, read]]",
                    transformId,
                    JUNIOR_USERNAME,
                    sourceIndexName,
                    destIndexName
                )
            )
        );

        putTransform(
            transformId,
            Strings.toString(config),
            RequestOptions.DEFAULT.toBuilder()
                .addHeader(AUTH_KEY, SENIOR_HEADER)
                .addParameter("defer_validation", String.valueOf(false))
                .build()
        );

        assertHealthAndAuthState(transformId, GREEN, GREEN, null);
    }

    /**
     * defer_validation        = true
     * unattended              = false
     * pre-existing dest index = false
     */
    @SuppressWarnings("unchecked")
    public void testTransformPermissionsDeferNoUnattendedNoDest() throws Exception {
        String transformId = "transform-permissions-defer-nounattended";
        String sourceIndexName = transformId + "-index";
        String destIndexName = sourceIndexName + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, false);
        putTransform(
            transformId,
            Strings.toString(config),
            RequestOptions.DEFAULT.toBuilder().addParameter("defer_validation", String.valueOf(true)).build()
        );
        String expectedErrorMessage = Strings.format(
            "Cannot create transform [%s] because user %s lacks the required permissions "
                + "[%s:[read, view_index_metadata], %s:[create_index, index, read]]",
            transformId,
            JUNIOR_USERNAME,
            sourceIndexName,
            destIndexName
        );
        assertHealthAndAuthState(transformId, GREEN, RED, expectedErrorMessage);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, JUNIOR_HEADER).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(e.getMessage(), containsString(expectedErrorMessage));

        assertHealthAndAuthState(transformId, GREEN, RED, expectedErrorMessage);

        e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(e.getMessage(), containsString(expectedErrorMessage));

        assertHealthAndAuthState(transformId, GREEN, RED, expectedErrorMessage);

        // update transform's credentials so that the transform has permission to access source/dest indices
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build());

        assertHealthAndAuthState(transformId, GREEN, GREEN, null);

        // _start API now works
        startTransform(config.getId(), RequestOptions.DEFAULT);
        waitUntilCheckpoint(transformId, 1);

        assertHealthAndAuthState(transformId, GREEN, GREEN, null);
    }

    /**
     * defer_validation        = true
     * unattended              = false
     * pre-existing dest index = true
     */
    @SuppressWarnings("unchecked")
    public void testTransformPermissionsDeferNoUnattendedDest() throws Exception {
        String transformId = "transform-permissions-defer-nounattended-dest-exists";
        String sourceIndexName = transformId + "-index";
        String destIndexName = sourceIndexName + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        createIndex(adminClient(), destIndexName, Settings.EMPTY);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, false);
        putTransform(
            transformId,
            Strings.toString(config),
            RequestOptions.DEFAULT.toBuilder().addParameter("defer_validation", String.valueOf(true)).build()
        );
        String expectedErrorMessage = Strings.format(
            "Cannot create transform [%s] because user %s lacks the required permissions "
                + "[%s:[read, view_index_metadata], %s:[index, read]]",
            transformId,
            JUNIOR_USERNAME,
            sourceIndexName,
            destIndexName
        );
        assertHealthAndAuthState(transformId, GREEN, RED, expectedErrorMessage);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, JUNIOR_HEADER).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(e.getMessage(), containsString(expectedErrorMessage));

        assertHealthAndAuthState(transformId, GREEN, RED, expectedErrorMessage);

        e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(e.getMessage(), containsString(expectedErrorMessage));

        assertHealthAndAuthState(transformId, GREEN, RED, expectedErrorMessage);

        // update transform's credentials so that the transform has permission to access source/dest indices
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build());

        assertHealthAndAuthState(transformId, GREEN, GREEN, null);

        // _start API now works
        startTransform(config.getId(), RequestOptions.DEFAULT);
        waitUntilCheckpoint(transformId, 1);

        assertHealthAndAuthState(transformId, GREEN, GREEN, null);
    }

    /**
     * defer_validation = true
     * unattended       = false
     */
    @SuppressWarnings("unchecked")
    public void testNoTransformAdminRoleInSecondaryAuth() throws Exception {
        String transformId = "transform-permissions-no-admin-role";
        String sourceIndexName = transformId + "-index";
        String destIndexName = sourceIndexName + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, false);

        // PUT with defer_validation should work even though the secondary auth does not have transform_admin role
        putTransform(
            transformId,
            Strings.toString(config),
            RequestOptions.DEFAULT.toBuilder()
                .addHeader(SECONDARY_AUTH_KEY, NOT_A_TRANSFORM_ADMIN_HEADER)
                .addParameter("defer_validation", String.valueOf(true))
                .build()
        );

        // _update should work even though the secondary auth does not have transform_admin role
        updateConfig(
            transformId,
            "{}",
            RequestOptions.DEFAULT.toBuilder().addHeader(SECONDARY_AUTH_KEY, NOT_A_TRANSFORM_ADMIN_HEADER).build()
        );

        // _start works because user not_a_transform_admin has data access
        startTransform(config.getId(), RequestOptions.DEFAULT);
    }

    /**
     * defer_validation        = true
     * unattended              = true
     * pre-existing dest index = false
     */
    @SuppressWarnings("unchecked")
    public void testTransformPermissionsDeferUnattendedNoDest() throws Exception {
        String transformId = "transform-permissions-defer-unattended";
        String sourceIndexName = transformId + "-index";
        String destIndexName = sourceIndexName + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, true);
        putTransform(
            transformId,
            Strings.toString(config),
            RequestOptions.DEFAULT.toBuilder().addParameter("defer_validation", String.valueOf(true)).build()
        );
        String expectedErrorMessage = Strings.format(
            "Cannot create transform [%s] because user %s lacks the required permissions "
                + "[%s:[read, view_index_metadata], %s:[create_index, index, read]]",
            transformId,
            JUNIOR_USERNAME,
            sourceIndexName,
            destIndexName
        );
        assertHealthAndAuthState(transformId, GREEN, RED, expectedErrorMessage);

        startTransform(config.getId(), RequestOptions.DEFAULT);

        // transform is yellow
        assertBusy(() -> {
            Map<String, Object> stats = getTransformStats(transformId);
            assertThat(extractValue(stats, "health", "status"), is(equalTo(YELLOW)));
            List<Object> issues = (List<Object>) extractValue(stats, "health", "issues");
            assertThat(issues, hasSize(1));
            assertThat(
                (String) extractValue((Map<String, Object>) issues.get(0), "details"),
                containsString(Strings.format("no such index [%s]", destIndexName))
            );
        }, 10, TimeUnit.SECONDS);

        // update transform's credentials so that the transform has permission to access source/dest indices
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build());
        waitUntilCheckpoint(transformId, 1);

        // transform is green again
        assertHealthAndAuthState(transformId, GREEN, GREEN, null);
    }

    /**
     * defer_validation        = true
     * unattended              = true
     * pre-existing dest index = true
     */
    @SuppressWarnings("unchecked")
    public void testTransformPermissionsDeferUnattendedDest() throws Exception {
        String transformId = "transform-permissions-defer-unattended-dest-exists";
        String sourceIndexName = transformId + "-index";
        String destIndexName = sourceIndexName + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        createIndex(adminClient(), destIndexName, Settings.EMPTY);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, true);
        putTransform(
            transformId,
            Strings.toString(config),
            RequestOptions.DEFAULT.toBuilder().addParameter("defer_validation", String.valueOf(true)).build()
        );
        String expectedErrorMessage = Strings.format(
            "Cannot create transform [%s] because user %s lacks the required permissions "
                + "[%s:[read, view_index_metadata], %s:[index, read]]",
            transformId,
            JUNIOR_USERNAME,
            sourceIndexName,
            destIndexName
        );
        assertHealthAndAuthState(transformId, GREEN, RED, expectedErrorMessage);

        startTransform(config.getId(), RequestOptions.DEFAULT);

        // transform's auth state status is still RED, but the health status is GREEN (because dest index exists)
        assertHealthAndAuthState(transformId, GREEN, RED, expectedErrorMessage);

        // update transform's credentials so that the transform has permission to access source/dest indices
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build());
        waitUntilCheckpoint(transformId, 1);

        // transform is green again
        assertHealthAndAuthState(transformId, GREEN, GREEN, null);
    }

    public void testPreviewRequestFailsPermissionsCheck() throws Exception {
        String transformId = "transform-permissions-preview";
        String sourceIndexName = transformId + "-index";
        String destIndexName = sourceIndexName + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, false);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> previewTransform(Strings.toString(config), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, JUNIOR_HEADER).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(
            e.getMessage(),
            containsString(
                Strings.format(
                    "Cannot preview transform [%s] because user %s lacks the required permissions "
                        + "[%s:[read, view_index_metadata], %s:[create_index, index, read]]",
                    transformId,
                    JUNIOR_USERNAME,
                    sourceIndexName,
                    destIndexName
                )
            )
        );

        previewTransform(Strings.toString(config), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build());
    }

    @Override
    protected Settings restAdminSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", TEST_ADMIN_HEADER).build();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", JUNIOR_HEADER).build();
    }

    private TransformConfig createConfig(String transformId, String sourceIndexName, String destIndexName, boolean unattended)
        throws Exception {
        Map<String, SingleGroupSource> groups = Map.of(
            "by-day",
            createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null),
            "by-user",
            new TermsGroupSource("user_id", null, false),
            "by-business",
            new TermsGroupSource("business_id", null, false)
        );

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config = createTransformConfigBuilder(transformId, destIndexName, QueryConfig.matchAll(), sourceIndexName)
            .setPivotConfig(createPivotConfig(groups, aggs))
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            .setSettings(new SettingsConfig.Builder().setAlignCheckpoints(false).setUnattended(unattended).build())
            .build();

        return config;
    }

    private void assertHealthAndAuthState(
        String transformId,
        String expectedHealthStatus,
        String expectedAuthStatus,
        String expectedLastAuthError
    ) throws IOException {
        Map<String, Object> stats = getTransformStats(transformId);
        assertThat("Stats were: " + stats, extractValue(stats, "health", "status"), is(equalTo(expectedHealthStatus)));
        assertThat("Stats were: " + stats, extractValue(stats, "auth_state", "status"), is(equalTo(expectedAuthStatus)));
        assertThat("Stats were: " + stats, extractValue(stats, "auth_state", "last_auth_error"), is(equalTo(expectedLastAuthError)));
    }
}
