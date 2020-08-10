/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.rabbitmq;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.xpack.watcher.common.http.Scheme;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQAccount {

    private static final String DEFAULT_EXCHANGE = "";

    static final String ISSUE_DEFAULTS_SETTING = "issue_defaults";
    static final String ALLOW_HTTP_SETTING = "allow_http";

    public static final Setting<SecureString> SECURE_USER_SETTING = SecureSetting.secureString("secure_user", null);
    public static final Setting<SecureString> SECURE_PASSWORD_SETTING = SecureSetting.secureString("secure_password", null);
    public static final Setting<SecureString> SECURE_URL_SETTING = SecureSetting.secureString("secure_url", null);

    private final String name;
    private final String user;
    private final String password;
    private final URI url;
    private ConnectionFactory connectionFactory;
    
    public RabbitMQAccount(String name, Settings settings, ConnectionFactory connectionFactory) {
        this.name = name;
        String url = getSetting(name, settings, SECURE_URL_SETTING);
        try {
            URI uri = new URI(url);
            if (uri.getScheme() == null) {
                throw new URISyntaxException("null", "No scheme defined in url");
            }
            Scheme protocol = Scheme.parse(uri.getScheme());
            if ((protocol == Scheme.HTTP) && (Booleans.isTrue(settings.get(ALLOW_HTTP_SETTING)) == false)) {
                throw new SettingsException("invalid RabbitMQ [" + name + "] account settings. unsecure scheme [" + protocol + "]");
            }
            this.url = uri;
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw new SettingsException(
                "invalid RabbitMQ [" + name + "] account settings. invalid [" + SECURE_URL_SETTING.getKey() + "] setting", e);
        }
        this.user = getSetting(name, settings, SECURE_USER_SETTING);
        this.password = getSetting(name, settings, SECURE_PASSWORD_SETTING);
        this.connectionFactory = connectionFactory;
    }

    private static String getSetting(String accountName, Settings settings, Setting<SecureString> secureSetting) {
        SecureString secureString = secureSetting.get(settings);
        if (secureString == null || secureString.length() < 1) {
            throw requiredSettingException(accountName, secureSetting.getKey());
        }
        return secureString.toString();
    }

    public String getName() {
        return name;
    }

    public void sendMessage(String exchange, String routingKey, final Map<String, Object> headers, 
            byte[] body) throws IOException, TimeoutException {
        
        connectionFactory.setUsername(user);
        connectionFactory.setPassword(password);
        connectionFactory.setHost(url.getHost());
        connectionFactory.setPort(url.getPort());
        
        Connection connection = null;
        Channel channel = null;
        try {
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
            if(!DEFAULT_EXCHANGE.equals(exchange)) {
                channel.exchangeDeclare(exchange, "direct", false);
            }
            
            channel.basicPublish(exchange, routingKey, new AMQP.BasicProperties.Builder()
                    .headers(headers).build(), body);
        } finally {
            if(channel != null)  {
                channel.close();
            }
            if(connection != null) {
                connection.close();
            }
        }
    }
    
    private static SettingsException requiredSettingException(String account, String setting) {
        return new SettingsException("invalid RabbitMQ [" + account + "] account settings. missing required [" + setting + "] setting");
    }
}
