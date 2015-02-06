/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.email;

import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

/**
 */
public class EmailSettingsService extends AbstractComponent implements NodeSettingsService.Listener {

    static final String PORT_SETTING = "alerts.action.email.server.port";
    static final String SERVER_SETTING = "alerts.action.email.server.name";
    static final String FROM_SETTING = "alerts.action.email.from.address";
    static final String USERNAME_SETTING = "alerts.action.email.from.username";
    static final String PASSWORD_SETTING = "alerts.action.email.from.password";

    private static final String DEFAULT_SERVER = "smtp.gmail.com";
    private static final int DEFAULT_PORT = 578;

    private volatile EmailServiceConfig emailServiceConfig = new EmailServiceConfig(DEFAULT_SERVER, DEFAULT_PORT, null, null, null);

    @Inject
    public EmailSettingsService(Settings settings, DynamicSettings dynamicSettings, NodeSettingsService nodeSettingsService) {
        super(settings);
        //TODO Add validators for hosts and email addresses
        dynamicSettings.addDynamicSetting(PORT_SETTING, Validator.POSITIVE_INTEGER);
        dynamicSettings.addDynamicSetting(SERVER_SETTING);
        dynamicSettings.addDynamicSetting(FROM_SETTING);
        dynamicSettings.addDynamicSetting(USERNAME_SETTING);
        dynamicSettings.addDynamicSetting(PASSWORD_SETTING);

        nodeSettingsService.addListener(this);

        updateSettings(settings);

    }

    public EmailServiceConfig emailServiceConfig() {
        return emailServiceConfig;
    }

    // This is useful to change all settings at the same time. Otherwise we may change the username then email gets send
    // and then change the password and then the email sending fails.
    //
    // Also this reduces the number of volatile writes
    static class EmailServiceConfig {

        private String host;
        private int port;
        private String username;
        private String password;
        private String defaultFromAddress;

        public String host() {
            return host;
        }

        public int port() {
            return port;
        }

        public String username() {
            return username;
        }

        public String password() {
            return password;
        }

        public String defaultFromAddress() {
            return defaultFromAddress;
        }

        private EmailServiceConfig(String host, int port, String userName, String password, String defaultFromAddress) {
            this.host = host;
            this.port = port;
            this.username = userName;
            this.password = password;
            this.defaultFromAddress = defaultFromAddress;

        }
    }

    @Override
    public void onRefreshSettings(Settings settings) {
        updateSettings(settings);
    }

    private void updateSettings(Settings settings) {
        boolean changed = false;
        String host = emailServiceConfig.host;
        String newHost = settings.get(SERVER_SETTING);
        if (newHost != null && !newHost.equals(host)) {
            logger.info("host changed from [{}] to [{}]", host, newHost);
            host = newHost;
            changed = true;
        }
        int port = emailServiceConfig.port;
        int newPort = settings.getAsInt(PORT_SETTING, -1);
        if (newPort != -1) {
            logger.info("port changed from [{}] to [{}]", port, newPort);
            port = newPort;
            changed = true;
        }
        String fromAddress = emailServiceConfig.defaultFromAddress;
        String newFromAddress = settings.get(FROM_SETTING);
        if (newFromAddress != null && !newFromAddress.equals(fromAddress)) {
            logger.info("from changed from [{}] to [{}]", fromAddress, newFromAddress);
            fromAddress = newFromAddress;
            changed = true;
        }
        String userName = emailServiceConfig.username;
        String newUserName = settings.get(USERNAME_SETTING);
        if (newUserName != null && !newUserName.equals(userName)) {
            logger.info("username changed from [{}] to [{}]", userName, newUserName);
            userName = newFromAddress;
            changed = true;
        }
        String password = emailServiceConfig.password;
        String newPassword = settings.get(PASSWORD_SETTING);
        if (newPassword != null && !newPassword.equals(password)) {
            logger.info("password changed");
            password = newPassword;
            changed = true;
        }
        if (changed) {
            logger.info("one or more settings have changed, updating the email service config");
            emailServiceConfig = new EmailServiceConfig(host, port, fromAddress, userName, password);
        }
    }


}
