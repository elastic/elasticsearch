/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class UsageReportingSettingsTests extends ESTestCase {

    private static Settings.Builder complete() {
        return Settings.builder()
            .put(UsageReportingSettings.ENABLED.getKey(), true)
            .put(UsageReportingSettings.URL.getKey(), "https://usage.example.com")
            .put(UsageReportingSettings.DEPLOYMENT_ID.getKey(), "d")
            .put(UsageReportingSettings.CLUSTER_ID.getKey(), "c")
            .put(UsageReportingSettings.PROVIDER.getKey(), "aws")
            .put(UsageReportingSettings.REGION.getKey(), "eu-west-1")
            .put(UsageReportingSettings.SSL_PREFIX + ".certificate", "client.crt")
            .put(UsageReportingSettings.SSL_PREFIX + ".key", "client.key");
    }

    public void testDefaults() {
        UsageReportingSettings settings = UsageReportingSettings.of(Settings.EMPTY);
        assertThat(settings.enabled(), is(false));
        assertThat(settings.period(), equalTo(TimeValue.timeValueMinutes(5)));
        assertThat(settings.batchSize(), equalTo(100));
        assertThat(settings.requestTimeout(), equalTo(TimeValue.timeValueSeconds(30)));
        assertThat(settings.pendingMax(), equalTo(1000));
        assertThat(settings.reportExempt(), is(false));
    }

    public void testCompleteConfigurationHasNothingMissing() {
        assertThat(UsageReportingSettings.of(complete().build()).problems(), empty());
    }

    public void testMissingNamesWhatIsAbsent() {
        Settings partial = Settings.builder()
            .put(UsageReportingSettings.ENABLED.getKey(), true)
            .put(UsageReportingSettings.CLUSTER_ID.getKey(), "c")
            .build();
        assertThat(
            UsageReportingSettings.of(partial).problems(),
            equalTo(
                List.of(
                    UsageReportingSettings.URL.getKey(),
                    UsageReportingSettings.DEPLOYMENT_ID.getKey(),
                    UsageReportingSettings.PROVIDER.getKey(),
                    UsageReportingSettings.REGION.getKey()
                )
            )
        );
    }

    public void testPeriodsThatDivideAnHourAreAccepted() {
        for (String period : List.of("1s", "5m", "15m", "1h", "30s")) {
            Settings s = Settings.builder().put(UsageReportingSettings.PERIOD.getKey(), period).build();
            assertThat(UsageReportingSettings.of(s).period(), equalTo(TimeValue.parseTimeValue(period, "test")));
        }
    }

    public void testPeriodsThatDoNotDivideAnHourAreRejected() {
        for (String period : List.of("7m", "2h", "0s", "1500ms", "11s")) {
            Settings s = Settings.builder().put(UsageReportingSettings.PERIOD.getKey(), period).build();
            Exception e = expectThrows(IllegalArgumentException.class, () -> UsageReportingSettings.of(s));
            assertThat(e.getMessage(), containsString("divides one hour"));
        }
    }

    public void testUnusableUrlLeavesThePluginInactiveInsteadOfFailingStartup() {
        for (String bad : List.of("usage.example.com", "https://", "not a url", "/relative/path", "https://usage_api.internal")) {
            Settings s = complete().put(UsageReportingSettings.URL.getKey(), bad).build();
            List<String> problems = UsageReportingSettings.of(s).problems();
            assertThat(problems, hasSize(1));
            assertThat(problems.get(0), startsWith(UsageReportingSettings.URL.getKey()));
        }
    }

    /** The problems are logged, so they must not carry what the url contains, such as credentials. */
    public void testProblemsDoNotRevealCredentialsOfTheUrl() {
        for (String bad : List.of(
            "http://user:secret@usage.example.com",
            "user:secret@usage.example.com",
            "admin:hunter2@usage.example.com",
            "ftp://admin:hunter2@usage.example.com",
            "https://user:secret@",
            "a b:secret"
        )) {
            Settings s = complete().put(UsageReportingSettings.URL.getKey(), bad).build();
            List<String> problems = UsageReportingSettings.of(s).problems();
            problems.forEach(p -> {
                assertThat(p, not(containsString("secret")));
                assertThat(p, not(containsString("hunter2")));
                assertThat(p, not(containsString("admin")));
            });
        }
    }

    public void testCredentialsInTheUrlAreRejectedWithAFixedMessage() {
        Settings s = complete().put(UsageReportingSettings.URL.getKey(), "https://user:secret@usage.example.com").build();
        List<String> problems = UsageReportingSettings.of(s).problems();
        assertThat(problems, hasSize(1));
        assertThat(
            problems.get(0),
            equalTo(UsageReportingSettings.URL.getKey() + " ([" + UsageReportingSettings.URL.getKey() + "] must not contain credentials)")
        );
    }

    public void testPortsOutsideTheRangeAreRejected() {
        for (String bad : List.of("https://usage.example.com:99999", "https://usage.example.com:65536", "https://usage.example.com:0")) {
            Settings s = complete().put(UsageReportingSettings.URL.getKey(), bad).build();
            List<String> problems = UsageReportingSettings.of(s).problems();
            assertThat(problems, hasSize(1));
            assertThat(problems.get(0), containsString("port"));
        }
        for (String ok : List.of("https://usage.example.com:1", "https://usage.example.com:65535")) {
            Settings s = complete().put(UsageReportingSettings.URL.getKey(), ok).build();
            assertThat(UsageReportingSettings.of(s).problems(), empty());
        }
    }

    public void testUrlIsFilteredFromTheSettingsOutput() {
        assertThat(UsageReportingSettings.URL.isFiltered(), equalTo(true));
    }

    /** The SSL loader treats an empty value as unset, so an empty keystore path or certificate is not a client certificate. */
    public void testEmptyCertificateSettingsAreNotACertificate() {
        Settings.Builder none = complete().putNull(UsageReportingSettings.SSL_PREFIX + ".certificate")
            .putNull(UsageReportingSettings.SSL_PREFIX + ".key");
        Settings emptyKeystore = Settings.builder().put(none.build()).put(UsageReportingSettings.SSL_PREFIX + ".keystore.path", "").build();
        assertThat(UsageReportingSettings.of(emptyKeystore).problems(), hasSize(1));
        Settings emptyPair = complete().put(UsageReportingSettings.SSL_PREFIX + ".certificate", "")
            .put(UsageReportingSettings.SSL_PREFIX + ".key", "")
            .build();
        assertThat(UsageReportingSettings.of(emptyPair).problems(), hasSize(1));
        // The loader treats a value with only whitespace as a file path, so it is set.
        Settings whitespace = complete().put(UsageReportingSettings.SSL_PREFIX + ".certificate", " ")
            .put(UsageReportingSettings.SSL_PREFIX + ".key", " ")
            .build();
        assertThat(UsageReportingSettings.of(whitespace).problems(), empty());
    }

    public void testUsableUrlsHaveNoProblem() {
        for (String ok : List.of("https://usage.example.com", "https://usage.example.com:8443/api/v1/usage", "HTTPS://usage.example.com")) {
            Settings s = complete().put(UsageReportingSettings.URL.getKey(), ok).build();
            assertThat(UsageReportingSettings.of(s).problems(), empty());
        }
    }

    public void testHttpsNeedsAClientCertificate() {
        Settings.Builder noCertificate = complete().putNull(UsageReportingSettings.SSL_PREFIX + ".certificate")
            .putNull(UsageReportingSettings.SSL_PREFIX + ".key");
        List<String> problems = UsageReportingSettings.of(noCertificate.build()).problems();
        assertThat(problems, hasSize(1));
        assertThat(problems.get(0), containsString(".ssl.certificate and xpack.esql.usage_reporting.ssl.key, or"));

        Settings keyOnly = complete().putNull(UsageReportingSettings.SSL_PREFIX + ".certificate").build();
        assertThat(UsageReportingSettings.of(keyOnly).problems(), hasSize(1));

        Settings keystore = noCertificate.put(UsageReportingSettings.SSL_PREFIX + ".keystore.path", "client.p12").build();
        assertThat(UsageReportingSettings.of(keystore).problems(), empty());
    }

    /** The http url of tests has no TLS, so it needs no certificate (whether or not the build accepts http at all). */
    public void testHttpUrlNeverAsksForACertificate() {
        Settings s = complete().put(UsageReportingSettings.URL.getKey(), "http://localhost:9999")
            .putNull(UsageReportingSettings.SSL_PREFIX + ".certificate")
            .putNull(UsageReportingSettings.SSL_PREFIX + ".key")
            .build();
        UsageReportingSettings.of(s).problems().forEach(p -> assertThat(p, not(containsString("certificate"))));
    }

    /** The setting itself reads the build type, which is a snapshot in tests, so both cases go through the overload. */
    public void testHttpIsAcceptedOnlyWhereAllowed() {
        UsageReportingSettings.validateUrl("http://localhost:9999", true);
        expectThrows(IllegalArgumentException.class, () -> UsageReportingSettings.validateUrl("http://localhost:9999", false));
        UsageReportingSettings.validateUrl("https://localhost:9999", false);
    }

    public void testEmptyUrlIsReportedByName() {
        Settings s = complete().put(UsageReportingSettings.URL.getKey(), "").build();
        assertThat(UsageReportingSettings.of(s).problems(), equalTo(List.of(UsageReportingSettings.URL.getKey())));
    }

    public void testBatchSizeRange() {
        for (int ok : new int[] { 1, 1000 }) {
            Settings s = Settings.builder().put(UsageReportingSettings.BATCH_SIZE.getKey(), ok).build();
            assertThat(UsageReportingSettings.of(s).batchSize(), equalTo(ok));
        }
        for (int bad : new int[] { 0, 1001 }) {
            Settings s = Settings.builder().put(UsageReportingSettings.BATCH_SIZE.getKey(), bad).build();
            expectThrows(IllegalArgumentException.class, () -> UsageReportingSettings.of(s));
        }
    }
}
