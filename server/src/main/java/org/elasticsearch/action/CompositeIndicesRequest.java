/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

/**
 * Marker interface that needs to be implemented by all {@link org.elasticsearch.action.ActionRequest} subclasses that are composed of
 * multiple sub-requests which relate to one or more indices.  A composite request is executed by its own transport action class
 * (e.g. {@link org.elasticsearch.action.search.TransportMultiSearchAction}), which goes through all sub-requests and delegates their
 * execution to the appropriate transport action (e.g. {@link org.elasticsearch.action.search.TransportSearchAction}) for each single item.
 */
public interface CompositeIndicesRequest {
}
