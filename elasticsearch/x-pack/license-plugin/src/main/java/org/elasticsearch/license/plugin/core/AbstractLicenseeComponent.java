/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A supporting base class for injectable Licensee components.
 */
public abstract class AbstractLicenseeComponent<T extends AbstractLicenseeComponent<T>> extends AbstractLifecycleComponent<T>
        implements Licensee {

    private final String id;
    private final LicenseeRegistry clientService;
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();

    // we initialize the licensee state to enabled with trial operation mode
    protected volatile Status status = Status.ENABLED;

    protected AbstractLicenseeComponent(Settings settings, String id, LicenseeRegistry clientService) {
        super(settings);
        this.id = id;
        this.clientService = clientService;
    }

    @Override
    protected void doStart() {
        clientService.register(this);
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    public final String id() {
        return id;
    }

    /**
     * @return the current status of this licensee (can never be null)
     */
    public Status getStatus() {
        return status;
    }

    public void add(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public void onChange(Status status) {
        this.status = status;
        logger.trace("[{}] is running in [{}] mode", id(), status);
        for (Listener listener : listeners) {
            listener.onChange(status);
        }
    }

    public interface Listener {
        void onChange(Status status);
    }

}
