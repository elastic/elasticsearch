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
import org.elasticsearch.xpack.core.transform.transforms.DestAlias;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;
import org.junit.After;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    private enum Users {
        TEST_ADMIN("x_pack_rest_user", List.of()),
        JUNIOR("john_junior", List.of("transform_admin")),
        SENIOR("bill_senior", List.of("transform_admin", "source_index_access", "dest_index_access")),
        SOURCE_AND_DEST_INDEX_ACCESS_ONLY("source_and_dest_index_access_only", List.of("source_index_access", "dest_index_access")),
        TRANSFORM_USER_BUT_NOT_ADMIN("transform_user_but_not_admin", List.of("transform_user", "source_index_access", "dest_index_access")),
        FLEET_ACCESS("fleet_access", List.of("transform_admin", "fleet_index_access", "dest_index_access"));

        private final String username;
        private final String effectiveRoles;
        private final String header;

        Users(String username, List<String> effectiveRoles) {
            this.username = username;
            this.effectiveRoles = effectiveRoles.stream().sorted().collect(Collectors.joining(","));
            this.header = basicAuthHeaderValue(username, TEST_PASSWORD_SECURE_STRING);
        }
    }

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
        String destIndexName = transformId + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, null, unattended);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> putTransform(
                transformId,
                Strings.toString(config),
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader(AUTH_KEY, Users.JUNIOR.header)
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
                        + "[%s:[create_index, index, read], %s:[read, view_index_metadata]]",
                    transformId,
                    Users.JUNIOR.username,
                    destIndexName,
                    sourceIndexName
                )
            )
        );

        putTransform(
            transformId,
            Strings.toString(config),
            RequestOptions.DEFAULT.toBuilder()
                .addHeader(AUTH_KEY, Users.SENIOR.header)
                .addParameter("defer_validation", String.valueOf(false))
                .build()
        );

        assertGreen(transformId);
    }

    public void testTransformWithDestinationAlias() throws Exception {
        String transformId = "transform-permissions-with-destination-alias";
        String sourceIndexName = transformId + "-index";
        String destIndexName = transformId + "-dest";
        String destAliasName = transformId + "-alias";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        TransformConfig config = createConfig(
            transformId,
            sourceIndexName,
            destIndexName,
            List.of(new DestAlias(destAliasName, false)),
            false
        );
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> putTransform(
                transformId,
                Strings.toString(config),
                RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, Users.SENIOR.header).build()
            )
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(
            e.getMessage(),
            containsString(
                Strings.format(
                    "Cannot create transform [%s] because user %s lacks the required permissions [%s:[manage], %s:[manage], %s:[]]",
                    transformId,
                    Users.SENIOR.username,
                    destAliasName,
                    destIndexName,
                    sourceIndexName
                )
            )
        );
    }

    /**
     * defer_validation        = true
     * unattended              = false
     * pre-existing dest index = false
     */
    public void testTransformPermissionsDeferNoUnattendedNoDest() throws Exception {
        String transformId = "transform-permissions-defer-nounattended";
        String sourceIndexName = transformId + "-index";
        String destIndexName = transformId + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, null, false);
        putTransform(
            transformId,
            Strings.toString(config),
            RequestOptions.DEFAULT.toBuilder().addParameter("defer_validation", String.valueOf(true)).build()
        );
        String authIssue = Strings.format(
            "Cannot create transform [%s] because user %s lacks the required permissions "
                + "[%s:[create_index, index, read], %s:[read, view_index_metadata]]",
            transformId,
            Users.JUNIOR.username,
            destIndexName,
            sourceIndexName
        );
        assertRed(transformId, authIssue);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, Users.JUNIOR.header).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(e.getMessage(), containsString(authIssue));

        assertRed(transformId, authIssue);

        e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, Users.SENIOR.header).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(e.getMessage(), containsString(authIssue));

        assertRed(transformId, authIssue);

        // update transform's credentials so that the transform has permission to access source/dest indices
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, Users.SENIOR.header).build());

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
        String destIndexName = transformId + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        createIndex(adminClient(), destIndexName, Settings.EMPTY);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, null, false);
        putTransform(
            transformId,
            Strings.toString(config),
            RequestOptions.DEFAULT.toBuilder().addParameter("defer_validation", String.valueOf(true)).build()
        );
        String authIssue = Strings.format(
            "Cannot create transform [%s] because user %s lacks the required permissions "
                + "[%s:[index, read], %s:[read, view_index_metadata]]",
            transformId,
            Users.JUNIOR.username,
            destIndexName,
            sourceIndexName
        );
        assertRed(transformId, authIssue);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, Users.JUNIOR.header).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(e.getMessage(), containsString(authIssue));

        assertRed(transformId, authIssue);

        e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, Users.SENIOR.header).build())
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(e.getMessage(), containsString(authIssue));

        assertRed(transformId, authIssue);

        // update transform's credentials so that the transform has permission to access source/dest indices
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, Users.SENIOR.header).build());

        assertGreen(transformId);

        // _start API now works
        startTransform(config.getId(), RequestOptions.DEFAULT);
        waitUntilCheckpoint(transformId, 1);

        assertGreen(transformId);
    }

    /**
     * defer_validation = false
     * unattended       = false
     */
    public void testNoTransformAdminRoleNoDeferNoUnattended() throws Exception {
        testNoTransformAdminRole(false, false);
    }

    /**
     * defer_validation = false
     * unattended       = true
     */
    public void testNoTransformAdminRoleNoDeferUnattended() throws Exception {
        testNoTransformAdminRole(false, true);
    }

    /**
     * defer_validation = true
     * unattended       = false
     */
    public void testNoTransformAdminRoleDeferNoUnattended() throws Exception {
        testNoTransformAdminRole(true, false);
    }

    /**
     * defer_validation = true
     * unattended       = true
     */
    public void testNoTransformAdminRoleDeferUnattended() throws Exception {
        testNoTransformAdminRole(true, true);
    }

    private void testNoTransformAdminRole(boolean deferValidation, boolean unattended) throws Exception {
        Users user = randomFrom(Users.SOURCE_AND_DEST_INDEX_ACCESS_ONLY, Users.TRANSFORM_USER_BUT_NOT_ADMIN);
        String transformId = "transform-permissions-no-transform-role";
        String sourceIndexName = transformId + "-index";
        String destIndexName = transformId + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, null, unattended);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> putTransform(
                transformId,
                Strings.toString(config),
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader(AUTH_KEY, user.header)
                    .addParameter("defer_validation", String.valueOf(deferValidation))
                    .build()
            )
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(
            e.getMessage(),
            containsString(
                Strings.format(
                    "action [cluster:admin/transform/put] is unauthorized for user [%s] with effective roles [%s], "
                        + "this action is granted by the cluster privileges [manage_data_frame_transforms,manage_transform,manage,all]",
                    user.username,
                    user.effectiveRoles
                )
            )
        );
    }

    /**
     * defer_validation = true
     * unattended       = false
     */
    public void testNoTransformAdminRoleInSecondaryAuth() throws Exception {
        Users user = randomFrom(Users.SOURCE_AND_DEST_INDEX_ACCESS_ONLY, Users.TRANSFORM_USER_BUT_NOT_ADMIN);
        String transformId = "transform-permissions-no-transform-role-in-sec-auth";
        String sourceIndexName = transformId + "-index";
        String destIndexName = transformId + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, null, false);

        // PUT with defer_validation should work even though the secondary auth does not have transform_admin nor transform_user role
        putTransform(
            transformId,
            Strings.toString(config),
            RequestOptions.DEFAULT.toBuilder()
                .addHeader(SECONDARY_AUTH_KEY, user.header)
                .addParameter("defer_validation", String.valueOf(true))
                .build()
        );

        // _update should work even though the secondary auth does not have transform_admin nor transform_user role
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(SECONDARY_AUTH_KEY, user.header).build());

        // _start works because user source_and_dest_index_access_only does have data access
        startTransform(config.getId(), RequestOptions.DEFAULT);
        waitUntilCheckpoint(transformId, 1);
    }

    /**
     * defer_validation        = true
     * unattended              = true
     * pre-existing dest index = false
     */
    public void testTransformPermissionsDeferUnattendedNoDest() throws Exception {
        String transformId = "transform-permissions-defer-unattended";
        String sourceIndexName = transformId + "-index";
        String destIndexName = transformId + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, null, true);
        putTransform(
            transformId,
            Strings.toString(config),
            RequestOptions.DEFAULT.toBuilder().addParameter("defer_validation", String.valueOf(true)).build()
        );
        String authIssue = Strings.format(
            "Cannot create transform [%s] because user %s lacks the required permissions "
                + "[%s:[create_index, index, read], %s:[read, view_index_metadata]]",
            transformId,
            Users.JUNIOR.username,
            destIndexName,
            sourceIndexName
        );
        assertRed(transformId, authIssue);

        startTransform(transformId, RequestOptions.DEFAULT);

        var permissionIssues = Strings.format(
            "org.elasticsearch.exception.ElasticsearchSecurityException: Cannot start transform [%s] because user lacks required permissions, "
                + "see privileges_check_failed issue for more details",
            transformId
        );
        // transform's auth state status is still RED due to:
        // - lacking permissions
        // - and the inability to start the indexer (which is also a consequence of lacking permissions)
        assertBusy(() -> { assertRed(transformId, authIssue, permissionIssues); });

        // update transform's credentials so that the transform has permission to access source/dest indices
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, Users.SENIOR.header).build());
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
        String destIndexName = transformId + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        createIndex(adminClient(), destIndexName, Settings.EMPTY);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, null, true);
        putTransform(
            transformId,
            Strings.toString(config),
            RequestOptions.DEFAULT.toBuilder().addParameter("defer_validation", String.valueOf(true)).build()
        );
        String authIssue = Strings.format(
            "Cannot create transform [%s] because user %s lacks the required permissions "
                + "[%s:[index, read], %s:[read, view_index_metadata]]",
            transformId,
            Users.JUNIOR.username,
            destIndexName,
            sourceIndexName
        );
        assertRed(transformId, authIssue);

        startTransform(config.getId(), RequestOptions.DEFAULT);

        var permissionIssues = Strings.format(
            "org.elasticsearch.exception.ElasticsearchSecurityException: Cannot start transform [%s] because user lacks required permissions, "
                + "see privileges_check_failed issue for more details",
            transformId
        );
        // transform's auth state status is still RED due to:
        // - lacking permissions
        // - and the inability to start the indexer (which is also a consequence of lacking permissions)
        assertBusy(() -> { assertRed(transformId, authIssue, permissionIssues); });

        // update transform's credentials so that the transform has permission to access source/dest indices
        updateConfig(transformId, "{}", RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, Users.SENIOR.header).build());
        waitUntilCheckpoint(transformId, 1);

        // transform is green again
        assertGreen(transformId);
    }

    public void testPreviewRequestFailsPermissionsCheck() throws Exception {
        String transformId = "transform-permissions-preview";
        String sourceIndexName = transformId + "-index";
        String destIndexName = transformId + "-dest";
        createReviewsIndex(sourceIndexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        TransformConfig config = createConfig(transformId, sourceIndexName, destIndexName, null, false);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> previewTransform(
                Strings.toString(config),
                RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, Users.JUNIOR.header).build()
            )
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(403)));
        assertThat(
            e.getMessage(),
            containsString(
                Strings.format(
                    "Cannot preview transform [%s] because user %s lacks the required permissions "
                        + "[%s:[create_index, index, read], %s:[read, view_index_metadata]]",
                    transformId,
                    Users.JUNIOR.username,
                    destIndexName,
                    sourceIndexName
                )
            )
        );

        previewTransform(Strings.toString(config), RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, Users.SENIOR.header).build());
    }

    public void testFleetIndicesAccess() throws Exception {
        String transformId = "transform-permissions-fleet";
        String sourceIndexPattern = ".fleet-agents*";
        String destIndexName = transformId + "-dest";

        TransformConfig config = createConfig(transformId, sourceIndexPattern, destIndexName, null, false);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> previewTransform(
                Strings.toString(config),
                RequestOptions.DEFAULT.toBuilder().addHeader(AUTH_KEY, Users.FLEET_ACCESS.header).build()
            )
        );
        // The _preview request got past the authorization step (which is what interests us in this test) but failed because the referenced
        // source indices do not exist.
        assertThat("Error was: " + e.getMessage(), e.getResponse().getStatusLine().getStatusCode(), is(equalTo(400)));
        assertThat(e.getMessage(), containsString("Source indices have been deleted or closed."));
    }

    @Override
    protected Settings restAdminSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", Users.TEST_ADMIN.header).build();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", Users.JUNIOR.header).build();
    }

    private TransformConfig createConfig(
        String transformId,
        String sourceIndexName,
        String destIndexName,
        List<DestAlias> aliases,
        boolean unattended
    ) throws Exception {
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

        TransformConfig config = TransformConfig.builder()
            .setId(transformId)
            .setSource(new SourceConfig(new String[] { sourceIndexName }, QueryConfig.matchAll(), Collections.emptyMap()))
            .setDest(new DestConfig(destIndexName, aliases, null))
            .setFrequency(TimeValue.timeValueSeconds(10))
            .setDescription("Test transform config id: " + transformId)
            .setPivotConfig(createPivotConfig(groups, aggs))
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            .setSettings(new SettingsConfig.Builder().setAlignCheckpoints(false).setUnattended(unattended).build())
            .build();

        return config;
    }

    private void assertGreen(String transformId) throws IOException {
        Map<String, Object> stats = getBasicTransformStats(transformId);
        assertThat("Stats were: " + stats, extractValue(stats, "health", "status"), is(equalTo(GREEN)));
        assertThat("Stats were: " + stats, extractValue(stats, "health", "issues"), is(nullValue()));
    }

    // We expect exactly the issues passed as "expectedHealthIssueDetails". Not more, not less.
    @SuppressWarnings("unchecked")
    private void assertRed(String transformId, String... expectedHealthIssueDetails) throws IOException {
        Map<String, Object> stats = getBasicTransformStats(transformId);
        assertThat("Stats were: " + stats, extractValue(stats, "health", "status"), is(equalTo(RED)));
        List<Object> issues = (List<Object>) extractValue(stats, "health", "issues");
        assertThat("Stats were: " + stats, issues, hasSize(expectedHealthIssueDetails.length));
        Set<String> actualHealthIssueDetailsSet = issues.stream()
            .map(issue -> (String) extractValue((Map<String, Object>) issue, "details"))
            .collect(toSet());
        assertThat("Stats were: " + stats, actualHealthIssueDetailsSet, containsInAnyOrder(expectedHealthIssueDetails));
        // We should not progress beyond the 0th checkpoint until we correctly configure the Transform.
        assertThat("Stats were: " + stats, getCheckpoint(stats), equalTo(0L));
    }
}
