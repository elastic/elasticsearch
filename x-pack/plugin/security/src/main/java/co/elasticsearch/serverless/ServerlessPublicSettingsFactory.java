/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package co.elasticsearch.serverless;

import org.elasticsearch.common.settings.PublicSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Set;
//it is here just for the draft

public class ServerlessPublicSettingsFactory implements org.elasticsearch.common.settings.PublicSettingsFactory {
    @Override
    public PublicSettings create(ThreadContext threadContext, Set<Setting<?>> serverlessPublicSettings) {
        return new ServerlessPublicSettings(threadContext, serverlessPublicSettings);
    }
}
