/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.license;

import org.elasticsearch.license.plugin.core.LicensesClientService;

import java.util.HashSet;
import java.util.Set;

/**
 * Serves as a registry of license event listeners and enables notifying them about the
 * different events.
 *
 * This class is required to serves as a bridge between the license service and any other
 * service that needs to recieve license events. The reason for that is that some services
 * that require such notifications also serves as a dependency for the licensing service
 * which introdues a circular dependency in guice (e.g. TransportService). This class then
 * serves as a bridge between the different services to eliminate such circular dependencies.
 */
public class LicenseEventsNotifier {

    private final Set<LicensesClientService.Listener> listeners = new HashSet<>();

    public void register(LicensesClientService.Listener listener) {
        listeners.add(listener);
    }

    protected void notifyEnabled() {
        for (LicensesClientService.Listener listener : listeners) {
            listener.onEnabled();
        }
    }

    protected void notifyDisabled() {
        for (LicensesClientService.Listener listener : listeners) {
            listener.onDisabled();
        }
    }

}
