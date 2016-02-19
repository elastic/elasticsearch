/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.validation;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.support.Exceptions;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class WatcherSettingsValidation extends AbstractLifecycleComponent<WatcherSettingsValidation> {

    private List<String> errors = new ArrayList<>();

    @Inject
    public WatcherSettingsValidation(Settings settings) {
        super(settings);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        validate();
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    public void addError(String setting, String reason) {
        errors.add(LoggerMessageFormat.format("", "invalid [{}] setting value [{}]. {}", setting, settings.get(setting), reason));
    }

    private void validate() throws ElasticsearchException {
        if (errors.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder("encountered invalid watcher settings:\n");
        for (String error : errors) {
            sb.append("- ").append(error).append("\n");
        }
        throw Exceptions.invalidSettings(sb.toString());
    }
}
