/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * A container lifecycle class that helps manage the start/stop order of other lifecycle components.
 * It is not uncommon that lifecycle classes have dependencies between them and must be started
 * and stopped in well-defined orders. However today the lifecycle management provided by
 * {@link org.elasticsearch.node.Node} does not allow defining such orders. In practice, they are
 * likely started and stopped in the same order as they are added to the component collection.
 * Such order is undesirable since more often than not we want start and stop orders to be exactly
 * the opposite. Until such feature is made available, we manage the orders explicitly in this class.
 */
public class StatelessComponents extends AbstractLifecycleComponent {

    // The translogReplicator enqueues tasks to objectStoreService.
    // On start time, objectStoreService must be started before translogReplicator.
    // On close/stop time, translogReplicator must be closed/stopped first to prevent enqueuing
    // before objectStoreService.
    private final TranslogReplicator translogReplicator;
    private final ObjectStoreService objectStoreService;

    public StatelessComponents(TranslogReplicator translogReplicator, ObjectStoreService objectStoreService) {
        this.translogReplicator = translogReplicator;
        this.objectStoreService = objectStoreService;
    }

    public TranslogReplicator getTranslogReplicator() {
        return translogReplicator;
    }

    public ObjectStoreService getObjectStoreService() {
        return objectStoreService;
    }

    @Override
    protected void doStart() {
        startComponents(List.of(objectStoreService, translogReplicator));
    }

    @Override
    protected void doStop() {
        stopComponents(List.of(translogReplicator, objectStoreService));

    }

    @Override
    protected void doClose() throws IOException {
        closeComponents(List.of(translogReplicator, objectStoreService));
    }

    /**
     * Start the given components in order. If exception is thrown when starting one of the components,
     * close all previously started components in reverse order before throwing exception.
     */
    static void startComponents(List<LifecycleComponent> components) {
        for (int i = 0; i < components.size(); i++) {
            LifecycleComponent component = components.get(i);
            try {
                component.start();
            } catch (Exception e) {
                if (i > 0) {
                    final List<LifecycleComponent> componentsToStop = new ArrayList<>(components.subList(0, i));
                    Collections.reverse(componentsToStop); // close started components in reverse order
                    try {
                        closeComponents(componentsToStop);
                    } catch (Exception suppressed) {
                        e.addSuppressed(suppressed);
                    }
                }
                throw e;
            }
        }
    }

    /**
     * Stop the components in order and accumulating exception (if any) along the way. Any error is thrown
     * at the end of the iteration.
     */
    static void stopComponents(List<LifecycleComponent> components) {
        operateOnComponents(components, LifecycleComponent::stop);
    }

    /**
     * Close the components in order and accumulating exception (if any) along the way. Any error is thrown
     * at the end of the iteration.
     */
    static void closeComponents(List<LifecycleComponent> components) {
        operateOnComponents(components, LifecycleComponent::close);
    }

    private static void operateOnComponents(List<LifecycleComponent> components, Consumer<LifecycleComponent> consumer) {
        RuntimeException exception = null;
        for (LifecycleComponent component : components) {
            try {
                consumer.accept(component);
            } catch (RuntimeException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }
}
