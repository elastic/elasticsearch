/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xcontent.ToXContentObject;

/**
 * Provides a mechanism for building a KNN query vector in an asynchronous manner during the rewrite phase
 */
public interface QueryVectorBuilder extends VersionedNamedWriteable, ToXContentObject {

    /**
     * Method for building a vector via the client. This method is called during RerwiteAndFetch.
     * Typical implementation for this method will:
     *  1. call some asynchronous client action
     *  2. Handle failure/success for that action (usually passing failure to the provided listener)
     *  3. Parse the success case and extract the query vector
     *  4. Pass the extracted query vector to the provided listener
     *
     * @param client for performing asynchronous actions against the cluster
     * @param listener listener to accept the created vector
     */
    void buildVector(Client client, ActionListener<float[]> listener);

}
