/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xpack.esql.action.EsqlIntoDestinationAction;

import java.util.concurrent.Executor;

/**
 * Authorizes the caller's write access to the destination of an {@code INTO} before anything about that destination is reported.
 * The checks that run during planning read cluster state, which security does not filter, and a {@code ROW} source needs no index
 * privilege that would stop the caller first. Without this step a caller who may not write could learn about a destination (whether
 * it exists, its ingest pipeline, its routing) from the errors.
 */
public interface IntoDestinationAuthorizer {
    /**
     * Fails with a 403 if the caller may not write to {@code destination}.
     *
     * @param destination the concrete name the write goes to, with date math already resolved
     */
    void authorize(String destination, ActionListener<Void> listener);

    /**
     * Authorizes through the security filter, with an {@link EsqlIntoDestinationAction} that runs as the caller, so that a denial is
     * the filter's own, and is audited by it like the write would be. Without security the action succeeds.
     * <p>
     * The decision is delivered on {@code executor}, since the filter can answer on a security thread, and planning continues from it
     * on the thread pool that it expects.
     */
    static IntoDestinationAuthorizer create(Client client, Executor executor) {
        return (destination, listener) -> {
            try {
                // The filter would read a pattern as more than the one index the write targets. This is about the name alone, so it
                // says nothing about any destination.
                MetadataCreateIndexService.validateIndexOrAliasName(destination, InvalidIndexNameException::new);
            } catch (InvalidIndexNameException e) {
                listener.onFailure(e);
                return;
            }
            client.execute(
                EsqlIntoDestinationAction.TYPE,
                new EsqlIntoDestinationAction.Request(destination),
                new ThreadedActionListener<>(executor, true, listener.map(ignored -> null))
            );
        };
    }
}
