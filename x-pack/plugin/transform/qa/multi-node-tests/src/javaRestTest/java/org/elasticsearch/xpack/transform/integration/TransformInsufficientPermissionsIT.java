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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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

        assertGreen(transformId);
    }

    /**
     * defer_validation        = true
     * unattended              = false
     * pre-existing dest index = false
     */
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
        String authIssue = Strings.format(
            "Cannot create transform [%s] because user %s lacks the required permissions "
                + "[%s:[read, view_index_metadata], %s:[create_index, index, read]]",
            transformId,
            JUNIOR_USERNAME,
            sourceIndexName,
            destIndexName
        );
        assertRed(transformId, authIssue);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, JUNIOR_HEADER).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(e.getMessage(), containsString(authIssue));

        assertRed(transformId, authIssue);

        e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(e.getMessage(), containsString(authIssue));

        assertRed(transformId, authIssue);

        // update transform's credentials so that the transform has permission to access source/dest indices
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build());

        assertGreen(transformId);

        // _start API now works
        startTransform(config.getId(), RequestOptions.DEFAULT);
        waitUntilCheckpoint(transformId, 1);

        assertGreen(transformId);
    }

    /**
     * defer_validation        = true
     * unattended              = false
     * pre-existing dest index = true
     */
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
        String authIssue = Strings.format(
            "Cannot create transform [%s] because user %s lacks the required permissions "
                + "[%s:[read, view_index_metadata], %s:[index, read]]",
            transformId,
            JUNIOR_USERNAME,
            sourceIndexName,
            destIndexName
        );
        assertRed(transformId, authIssue);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, JUNIOR_HEADER).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(e.getMessage(), containsString(authIssue));

        assertRed(transformId, authIssue);

        e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(e.getMessage(), containsString(authIssue));

        assertRed(transformId, authIssue);

        // update transform's credentials so that the transform has permission to access source/dest indices
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build());

        assertGreen(transformId);

        // _start API now works
        startTransform(config.getId(), RequestOptions.DEFAULT);
        waitUntilCheckpoint(transformId, 1);

        assertGreen(transformId);
    }

    /**
     * defer_validation = true
     * unattended       = false
     */
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
        String authIssue = Strings.format(
            "Cannot create transform [%s] because user %s lacks the required permissions "
                + "[%s:[read, view_index_metadata], %s:[create_index, index, read]]",
            transformId,
            JUNIOR_USERNAME,
            sourceIndexName,
            destIndexName
        );
        assertRed(transformId, authIssue);

        startTransform(config.getId(), RequestOptions.DEFAULT);

        // transform is red with two issues
        String noSuchIndexIssue = Strings.format("org.elasticsearch.index.IndexNotFoundException: no such index [%s]", destIndexName);
        assertBusy(() -> assertRed(transformId, authIssue, noSuchIndexIssue), 10, TimeUnit.SECONDS);

        // update transform's credentials so that the transform has permission to access source/dest indices
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build());
        waitUntilCheckpoint(transformId, 1);

        // transform is green again
        assertGreen(transformId);
    }

    /**
     * defer_validation        = true
     * unattended              = true
     * pre-existing dest index = true
     */
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
        String authIssue = Strings.format(
            "Cannot create transform [%s] because user %s lacks the required permissions "
                + "[%s:[read, view_index_metadata], %s:[index, read]]",
            transformId,
            JUNIOR_USERNAME,
            sourceIndexName,
            destIndexName
        );
        assertRed(transformId, authIssue);

        startTransform(config.getId(), RequestOptions.DEFAULT);

        // transform's auth state status is still RED, but the health status is GREEN (because dest index exists)
        assertRed(transformId, authIssue);

        // update transform's credentials so that the transform has permission to access source/dest indices
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, SENIOR_HEADER).build());
        waitUntilCheckpoint(transformId, 1);

        // transform is green again
        assertGreen(transformId);
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

    private void assertGreen(String transformId) throws IOException {
        Map<String, Object> stats = getTransformStats(transformId);
        assertThat("Stats were: " + stats, extractValue(stats, "health", "status"), is(equalTo(GREEN)));
        assertThat("Stats were: " + stats, extractValue(stats, "health", "issues"), is(nullValue()));
    }

    @SuppressWarnings("unchecked")
    private void assertRed(String transformId, String... expectedHealthIssueDetails) throws IOException {
        Map<String, Object> stats = getTransformStats(transformId);
        assertThat("Stats were: " + stats, extractValue(stats, "health", "status"), is(equalTo(RED)));
        List<Object> issues = (List<Object>) extractValue(stats, "health", "issues");
        assertThat("Stats were: " + stats, issues, hasSize(expectedHealthIssueDetails.length));
        Set<String> actualHealthIssueDetailsSet = issues.stream()
            .map(issue -> (String) extractValue((Map<String, Object>) issue, "details"))
            .collect(toSet());
        assertThat("Stats were: " + stats, actualHealthIssueDetailsSet, containsInAnyOrder(expectedHealthIssueDetails));
    }
}
