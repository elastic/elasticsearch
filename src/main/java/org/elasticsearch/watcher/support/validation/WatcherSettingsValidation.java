/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.validation;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;

import java.util.*;

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
        WatcherSettingsException exception = new WatcherSettingsException();
        for (String error : errors) {
            exception.addError(error);
        }
        throw exception;
    }
}
