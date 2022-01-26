/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support.expressiondsl;

import org.elasticsearch.xcontent.ToXContentObject;

/**
 * Implementations of this interface represent an expression used for user role mapping
 * that can later be resolved to a boolean value.
 */
public interface RoleMapperExpression extends ToXContentObject {

}
