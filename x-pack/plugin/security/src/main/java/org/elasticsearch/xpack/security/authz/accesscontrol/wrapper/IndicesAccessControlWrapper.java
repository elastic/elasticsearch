/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol.wrapper;

import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

/**
 * A wrapper which allows adding additional functionality and intercepting calls to {@link IndicesAccessControl} methods.
 */
public interface IndicesAccessControlWrapper {

    IndicesAccessControl wrap(IndicesAccessControl indicesAccessControl);

}
