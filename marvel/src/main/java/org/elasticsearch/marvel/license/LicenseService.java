/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.mode.Mode;

import java.util.*;

public class LicenseService extends AbstractLifecycleComponent<LicenseService> {

    public static final String FEATURE_NAME = MarvelPlugin.NAME;

    private static final LicensesService.TrialLicenseOptions TRIAL_LICENSE_OPTIONS =
            new LicensesService.TrialLicenseOptions(TimeValue.timeValueHours(30 * 24), 1000);

    private static final FormatDateTimeFormatter DATE_FORMATTER = Joda.forPattern("EEEE, MMMMM dd, yyyy", Locale.ROOT);

    static final TimeValue GRACE_PERIOD = days(7);

    private final LicensesManagerService managerService;
    private final LicensesClientService clientService;
    private final MarvelSettings marvelSettings;
    private final Collection<LicensesService.ExpirationCallback> expirationLoggers;
    private final LicensesClientService.AcknowledgementCallback acknowledgementCallback;

    private volatile Mode mode;
    private volatile boolean enabled;
    private volatile long expiryDate;

    @Inject
    public LicenseService(Settings settings, LicensesClientService clientService, LicensesManagerService managerService, MarvelSettings marvelSettings) {
        super(settings);
        this.managerService = managerService;
        this.clientService = clientService;
        this.marvelSettings = marvelSettings;
        this.mode = Mode.LITE;
        this.expirationLoggers = Arrays.asList(
                new LicensesService.ExpirationCallback.Pre(days(7), days(30), days(1)) {
                    @Override
                    public void on(License license, LicensesService.ExpirationStatus status) {
                        logger.error("\n" +
                                "#\n" +
                                "# Marvel license will expire on [{}].\n" +
                                "# Have a new license? please update it. Otherwise, please reach out to your support contact.\n" +
                                "#", DATE_FORMATTER.printer().print(license.expiryDate()));
                    }
                },
                new LicensesService.ExpirationCallback.Pre(days(0), days(7), minutes(10)) {
                    @Override
                    public void on(License license, LicensesService.ExpirationStatus status) {
                        logger.error("\n" +
                                "#\n" +
                                "# Marvel license will expire on [{}].\n" +
                                "# Have a new license? please update it. Otherwise, please reach out to your support contact.\n" +
                                "#", DATE_FORMATTER.printer().print(license.expiryDate()));
                    }
                },
                new LicensesService.ExpirationCallback.Post(days(0), GRACE_PERIOD, minutes(10)) {
                    @Override
                    public void on(License license, LicensesService.ExpirationStatus status) {
                        long endOfGracePeriod = license.expiryDate() + GRACE_PERIOD.getMillis();
                        logger.error("\n" +
                                "#\n" +
                                "# MARVEL LICENSE HAS EXPIRED ON [{}].\n" +
                                "# MARVEL WILL STOP COLLECTING DATA ON [{}].\n" +
                                "# HAVE A NEW LICENSE? PLEASE UPDATE IT. OTHERWISE, PLEASE REACH OUT TO YOUR SUPPORT CONTACT.\n" +
                                "#", DATE_FORMATTER.printer().print(endOfGracePeriod), DATE_FORMATTER.printer().print(license.expiryDate()));
                    }
                }
        );
        this.acknowledgementCallback = new LicensesClientService.AcknowledgementCallback() {
            @Override
            public List<String> acknowledge(License currentLicense, License newLicense) {
                switch (newLicense.type()) {

                    case "trial":
                    case "gold":
                    case "platinum":
                        return Collections.emptyList();

                    default: // "basic" - we also fall back to basic for an unknown type
                        return Collections.singletonList(LoggerMessageFormat.format(
                                "Marvel: Multi-cluster support is disabled for clusters with [{}] licenses.\n" +
                                "If you are running multiple customers, users won't be able to access this\n" +
                                "all the clusters with [{}] licenses from a single Marvel instance. To access them\n" +
                                "a dedicated and separated marvel instance will be required for each cluster",
                                newLicense.type(), newLicense.type()));
                }
            }
        };
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        clientService.register(FEATURE_NAME, TRIAL_LICENSE_OPTIONS, expirationLoggers, acknowledgementCallback, new InternalListener(this));
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

    /**
     * @return the current marvel's operating mode
     */
    public Mode mode() {
        return mode;
    }

    /**
     * @return all registered licenses
     */
    public List<License> licenses() {
        return managerService.getLicenses();
    }

    /**
     * @return true if the marvel license is enabled
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * @return true if marvel is running within the "grace period", ie when the license
     * is expired but a given extra delay is not yet elapsed
     */
    public boolean inExpirationGracePeriod() {
        return System.currentTimeMillis() <= (expiryDate() + marvelSettings.licenseExpirationGracePeriod().millis());
    }

    /**
     * @return the license's expiration date (as a long)
     */
    public long expiryDate() {
        return expiryDate;
    }

    class InternalListener implements LicensesClientService.Listener {

        private final LicenseService service;

        public InternalListener(LicenseService service) {
            this.service = service;
        }

        @Override
        public void onEnabled(License license) {
            try {
                service.enabled = true;
                service.expiryDate = license.expiryDate();
                service.mode = Mode.fromName(license.type());
            } catch (IllegalArgumentException e) {
                service.mode = Mode.LITE;
            }
        }

        @Override
        public void onDisabled(License license) {
            service.enabled = false;
            service.expiryDate = license.expiryDate();
            service.mode = Mode.LITE;
        }
    }
}
