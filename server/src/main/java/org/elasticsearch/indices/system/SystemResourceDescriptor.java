/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.system;

import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.IndexMatcher;
import org.elasticsearch.indices.SystemIndexDescriptor;

import java.util.List;

public interface SystemResourceDescriptor extends IndexMatcher {
    /**
     * @return A short description of the purpose of this system resource.
     */
    String getDescription();

    boolean isAutomaticallyManaged();

    /**
     * Get an origin string suitable for use in an {@link org.elasticsearch.client.internal.OriginSettingClient}. See
     * {@link SystemIndexDescriptor.Builder#setOrigin(String)} for more information.
     *
     * @return an origin string to use for sub-requests
     */
    String getOrigin();

    boolean isExternal();

    /**
     * Requests from these products, if made with the proper security credentials, are allowed non-deprecated access to this descriptor's
     * indices. (Product names may be specified in requests with the
     * {@link org.elasticsearch.tasks.Task#X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER}).
     * @return A list of product names.
     */
    List<String> getAllowedElasticProductOrigins();

    /**
     * @return The names of thread pools that should be used for operations on this system index.
     */
    ExecutorNames getThreadPoolNames();
}
