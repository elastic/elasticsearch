/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.rabbitmq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.watcher.notification.NotificationService;

import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQService  extends NotificationService<RabbitMQAccount> {


    private static final Setting.AffixSetting<Boolean> SETTING_ALLOW_HTTP =
            Setting.affixKeySetting("xpack.notification.rabbitmq.account.", "allow_http",
                    (key) -> Setting.boolSetting(key, false, Property.Dynamic, Property.NodeScope));

    private static final Setting.AffixSetting<SecureString> SETTING_SECURE_USER =
            Setting.affixKeySetting("xpack.notification.rabbitmq.account.", "secure_user",
                    (key) -> SecureSetting.secureString(key, null));

    private static final Setting.AffixSetting<SecureString> SETTING_SECURE_URL =
            Setting.affixKeySetting("xpack.notification.rabbitmq.account.", "secure_url",
                    (key) -> SecureSetting.secureString(key, null));

    private static final Setting.AffixSetting<SecureString> SETTING_SECURE_PASSWORD =
            Setting.affixKeySetting("xpack.notification.rabbitmq.account.", "secure_password",
                    (key) -> SecureSetting.secureString(key, null));
    
    private ConnectionFactory connectionFactory;

    public RabbitMQService(Settings settings, ConnectionFactory connectionFactory, ClusterSettings clusterSettings) {
        super("rabbitmq", settings, clusterSettings, RabbitMQService.getDynamicSettings(), RabbitMQService.getSecureSettings());
        this.connectionFactory = connectionFactory;
        clusterSettings.addAffixUpdateConsumer(SETTING_ALLOW_HTTP, (s, o) -> {}, (s, o) -> {});
        reload(settings);
    }

    @Override
    protected RabbitMQAccount createAccount(String name, Settings settings) {
        return new RabbitMQAccount(name, settings, connectionFactory);
    }

    private static List<Setting<?>> getDynamicSettings() {
        return Arrays.asList(SETTING_ALLOW_HTTP);
    }

    private static List<Setting<?>> getSecureSettings() {
        return Arrays.asList(SETTING_SECURE_USER, SETTING_SECURE_PASSWORD, SETTING_SECURE_URL);
    }
    
    public static List<Setting<?>> getSettings() {
        List<Setting<?>> allSettings = new ArrayList<Setting<?>>(getDynamicSettings());
        allSettings.addAll(getSecureSettings());
        return allSettings;
    }
    
}
