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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.mode.Mode;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class LicenseService extends AbstractLifecycleComponent<LicenseService> {

    public static final String FEATURE_NAME = MarvelPlugin.NAME;

    private static final LicensesService.TrialLicenseOptions TRIAL_LICENSE_OPTIONS =
            new LicensesService.TrialLicenseOptions(TimeValue.timeValueHours(30 * 24), 1000);

    private static final FormatDateTimeFormatter DATE_FORMATTER = Joda.forPattern("EEEE, MMMMM dd, yyyy", Locale.ROOT);

    private final LicensesManagerService managerService;
    private final LicensesClientService clientService;
    private final Collection<LicensesService.ExpirationCallback> expirationLoggers;

    private volatile Mode mode;

    @Inject
    public LicenseService(Settings settings, LicensesClientService clientService, LicensesManagerService managerService) {
        super(settings);
        this.managerService = managerService;
        this.clientService = clientService;
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
                new LicensesService.ExpirationCallback.Post(days(0), null, minutes(10)) {
                    @Override
                    public void on(License license, LicensesService.ExpirationStatus status) {
                        logger.error("\n" +
                                "#\n" +
                                "# MARVEL LICENSE WAS EXPIRED ON [{}].\n" +
                                "# HAVE A NEW LICENSE? PLEASE UPDATE IT. OTHERWISE, PLEASE REACH OUT TO YOUR SUPPORT CONTACT.\n" +
                                "#", DATE_FORMATTER.printer().print(license.expiryDate()));
                    }
                }
        );
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        clientService.register(FEATURE_NAME, TRIAL_LICENSE_OPTIONS, expirationLoggers, new InternalListener(this));
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

    class InternalListener implements LicensesClientService.Listener {

        private final LicenseService service;

        public InternalListener(LicenseService service) {
            this.service = service;
        }

        @Override
        public void onEnabled(License license) {
            try {
                service.mode = Mode.fromName(license.type());
            } catch (IllegalArgumentException e) {
                service.mode = Mode.LITE;
            }
        }

        @Override
        public void onDisabled(License license) {
            service.mode = Mode.LITE;
        }
    }
}
