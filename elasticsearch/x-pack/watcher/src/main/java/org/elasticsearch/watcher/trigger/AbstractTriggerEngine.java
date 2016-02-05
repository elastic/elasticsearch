/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public abstract class AbstractTriggerEngine<T extends Trigger, E extends TriggerEvent> extends AbstractComponent implements
        TriggerEngine<T, E> {

    protected final List<Listener> listeners = new CopyOnWriteArrayList<>();

    public AbstractTriggerEngine(Settings settings) {
        super(settings);
    }

    @Override
    public void register(Listener listener) {
        listeners.add(listener);
    }
}
