/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.transport.TransportService;

/**
 * Represents a type of action that can be invoked by {@link Client#execute}. An {@code ActionType} serves
 * as a unique identifier and type descriptor for actions in the Elasticsearch action framework.
 *
 * <p>The implementation must be registered with the node using {@link ActionModule#setupActions}
 * (for actions in the {@code :server} package) or {@link ActionPlugin#getActions} (for actions in plugins).
 *
 * <p>Typically, every {@link ActionType} instance is a global constant (i.e. a public static final field)
 * called {@code INSTANCE} or {@code TYPE}. Some legacy implementations create custom subclasses of
 * {@link ActionType} but this is unnecessary and somewhat wasteful. Prefer to create instances of this
 * class directly whenever possible.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Define an action type as a public constant
 * public class MyCustomAction extends ActionType<MyCustomResponse> {
 *     public static final ActionType<MyCustomResponse> INSTANCE =
 *         new ActionType<>("cluster:admin/mycustom");
 * }
 *
 * // Execute the action using a client
 * client.execute(MyCustomAction.INSTANCE, request, listener);
 *
 * // Register the action in a plugin
 * public class MyPlugin extends Plugin implements ActionPlugin {
 *     @Override
 *     public List<ActionHandler<?, ?>> getActions() {
 *         return List.of(
 *             new ActionHandler<>(MyCustomAction.INSTANCE, TransportMyCustomAction.class)
 *         );
 *     }
 * }
 * }</pre>
 *
 * @param <Response> the type of response this action produces
 */
@SuppressWarnings("unused") // Response type arg is used to enable better type inference when calling Client#execute
public class ActionType<Response extends ActionResponse> {

    /**
     * Construct an {@link ActionType} with the given name.
     *
     * <p>There is no facility for directly executing an action on a different node in the local cluster.
     * To achieve this, implement an action which runs on the local node and knows how to use the
     * {@link TransportService} to forward the request to a different node. There are several utilities
     * that help implement such an action, including {@link TransportNodesAction} or
     * {@link TransportMasterNodeAction}.
     *
     * @param name The name of the action, which must be unique across actions.
     * @param <T> the type of response this action produces
     * @return an {@link ActionType} which callers can execute on the local node.
     * @deprecated Just create the {@link ActionType} directly using the constructor.
     */
    @Deprecated(forRemoval = true)
    public static <T extends ActionResponse> ActionType<T> localOnly(String name) {
        return new ActionType<>(name);
    }

    private final String name;

    /**
     * Constructs an {@link ActionType} with the given name.
     *
     * <p>There is no facility for directly executing an action on a different node in the local cluster.
     * To achieve this, implement an action which runs on the local node and knows how to use the
     * {@link TransportService} to forward the request to a different node. There are several utilities
     * that help implement such an action, including {@link TransportNodesAction} or
     * {@link TransportMasterNodeAction}.
     *
     * <p>Action names typically follow a hierarchical pattern like:
     * <ul>
     *   <li>{@code indices:data/read/search} for data operations</li>
     *   <li>{@code cluster:admin/settings/update} for admin operations</li>
     *   <li>{@code indices:admin/create} for index administration</li>
     * </ul>
     *
     * @param name The name of the action, which must be unique across all actions in the cluster.
     */
    public ActionType(String name) {
        this.name = name;
    }

    /**
     * Returns the unique name of this action.
     *
     * @return the action name, which is unique across all actions
     */
    public String name() {
        return this.name;
    }

    /**
     * Compares this action type with another object for equality. Two action types are equal
     * if they have the same name.
     *
     * @param o the object to compare with
     * @return {@code true} if the objects are equal, {@code false} otherwise
     */
    @Override
    public boolean equals(Object o) {
        return o instanceof ActionType<?> actionType && name.equals(actionType.name);
    }

    /**
     * Returns a hash code value for this action type based on its name.
     *
     * @return the hash code value
     */
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * Returns the string representation of this action type, which is its name.
     *
     * @return the action name
     */
    @Override
    public String toString() {
        return name;
    }
}
