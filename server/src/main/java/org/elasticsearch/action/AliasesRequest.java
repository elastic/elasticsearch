/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

/**
 * Needs to be implemented by all {@link org.elasticsearch.action.ActionRequest} subclasses that relate to
 * one or more indices and one or more aliases. Meant to be used for aliases management requests (e.g. add/remove alias,
 * get aliases) that hold aliases and indices in separate fields.
 * Allows to retrieve which indices and aliases the action relates to.
 */
public interface AliasesRequest extends IndicesRequest.Replaceable {

    /**
     * Returns the array of aliases that the action relates to
     */
    String[] aliases();

    /**
     * Returns the aliases as they were originally requested, before any potential name resolution
     */
    String[] getOriginalAliases();

    /**
     * Replaces current aliases with the provided aliases.
     *
     * Sometimes aliases expressions need to be resolved to concrete aliases prior to executing the transport action.
     */
    void replaceAliases(String... aliases);

    /**
     * Returns true if wildcards expressions among aliases should be resolved, false otherwise
     */
    boolean expandAliasesWildcards();
}
