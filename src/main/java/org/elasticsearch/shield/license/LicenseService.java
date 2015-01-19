/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.shield.ShieldPlugin;

import java.util.Collection;
import java.util.Locale;

/**
 *
 */
public class LicenseService extends AbstractLifecycleComponent<LicenseService> {

    public static final String FEATURE_NAME = ShieldPlugin.NAME;

    private static final LicensesService.TrialLicenseOptions TRIAL_LICENSE_OPTIONS =
            new LicensesService.TrialLicenseOptions(TimeValue.timeValueHours(30 * 24), 1000);

    private static final FormatDateTimeFormatter DATE_FORMATTER = Joda.forPattern("EEEE, MMMMM dd, yyyy", Locale.ROOT);

    private final LicensesClientService licensesClientService;
    private final LicenseEventsNotifier notifier;
    private final Collection<LicensesService.ExpirationCallback> expirationLoggers;

    private boolean enabled = false;

    @Inject
    public LicenseService(Settings settings, LicensesClientService licensesClientService, LicenseEventsNotifier notifier) {
        super(settings);
        this.licensesClientService = licensesClientService;
        this.notifier = notifier;
        this.expirationLoggers = ImmutableList.of(
                new LicensesService.ExpirationCallback.Pre(days(7), days(30), days(1)) {
                    @Override
                    public void on(License license, LicensesService.ExpirationStatus status) {
                        logger.error("\n" +
                                "#\n" +
                                "# Shield license will expire on [{}]. Cluster health, cluster stats and indices stats operations are\n" +
                                "# blocked on Shield license expiration. All data operations (read and write) continue to work. If you\n" +
                                "# have a new license, please update it. Otherwise, please reach out to your support contact.\n" +
                                "#", DATE_FORMATTER.printer().print(license.expiryDate()));
                    }
                },
                new LicensesService.ExpirationCallback.Pre(days(0), days(7), minutes(10)) {
                    @Override
                    public void on(License license, LicensesService.ExpirationStatus status) {
                        logger.error("\n" +
                                "#\n" +
                                "# Shield license will expire on [{}]. Cluster health, cluster stats and indices stats operations are\n" +
                                "# blocked on Shield license expiration. All data operations (read and write) continue to work. If you\n" +
                                "# have a new license, please update it. Otherwise, please reach out to your support contact.\n" +
                                "#", DATE_FORMATTER.printer().print(license.expiryDate()));
                    }
                },
                new LicensesService.ExpirationCallback.Post(days(0), null, minutes(10)) {
                    @Override
                    public void on(License license, LicensesService.ExpirationStatus status) {
                        logger.error("\n" +
                                "#\n" +
                                "# SHIELD LICENSE EXPIRED ON [{}]! CLUSTER HEALTH, CLUSTER STATS AND INDICES STATS OPERATIONS ARE\n" +
                                "# NOW BLOCKED. ALL DATA OPERATIONS (READ AND WRITE) CONTINUE TO WORK. IF YOU HAVE A NEW LICENSE, PLEASE\n" +
                                "# UPDATE IT. OTHERWISE, PLEASE REACH OUT TO YOUR SUPPORT CONTACT.\n" +
                                "#", DATE_FORMATTER.printer().print(license.expiryDate()));
                    }
                }
        );
    }

    public synchronized boolean enabled() {
        return enabled;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (settings.getGroups("tribe", true).isEmpty()) {
            licensesClientService.register(FEATURE_NAME, TRIAL_LICENSE_OPTIONS, expirationLoggers, new InternalListener());
        } else {
            //TODO currently we disable licensing on tribe node. remove this once es core supports merging cluster
            new InternalListener().onEnabled(null);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    static TimeValue days(int days) {
        return TimeValue.timeValueHours(days * 24);
    }

    static TimeValue minutes(int minutes) {
        return TimeValue.timeValueMinutes(minutes);
    }

    class InternalListener implements LicensesClientService.Listener {

        @Override
        public void onEnabled(License license) {
            synchronized (LicenseService.this) {
                logger.info("enabling license for [{}]", FEATURE_NAME);
                enabled = true;
                notifier.notifyEnabled();
            }
        }

        @Override
        public void onDisabled(License license) {
            synchronized (LicenseService.this) {
                logger.info("DISABLING LICENSE FOR [{}]", FEATURE_NAME);
                enabled = false;
                notifier.notifyDisabled();
            }
        }
    }

}
