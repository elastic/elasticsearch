/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package co.elasticsearch.serverless;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.PublicSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.PRIVILEGE_CATEGORY_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR;

//it is here just for the draft
public class ServerlessPublicSettings implements PublicSettings {

    private final ThreadContext threadContext;
    private final Set<String> orchestratorSettingsKeys;

    public ServerlessPublicSettings(ThreadContext threadContext, Set<Setting<?>> orchestratorSettings) {
        this.threadContext = threadContext;
        this.orchestratorSettingsKeys = orchestratorSettings.stream().map(Setting::getKey).collect(Collectors.toSet());
    }

    @Override
    public Settings filterPublic(Settings settings) {
        if (false == PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(threadContext.getHeader(PRIVILEGE_CATEGORY_KEY))) {
            return settings.filter(key -> orchestratorSettingsKeys.contains(key));
        }
        return settings;
    }

    @Override
    public void validateSettings(Settings settings) {
        if (false == PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(threadContext.getHeader(PRIVILEGE_CATEGORY_KEY))) {
            List<String> list = settings.keySet()
                .stream()
                .filter(settingName -> orchestratorSettingsKeys.contains(settingName) == false)
                .toList();
            if (false == list.isEmpty()) {
                throw new IllegalArgumentException(
                    "unknown "
                        + (list.size() == 1 ? "setting" : "settings")
                        + " ["
                        + Strings.collectionToDelimitedString(list, ",")
                        + "]"
                        + " within non-operator mode"
                );
            }
        }

    }

}
