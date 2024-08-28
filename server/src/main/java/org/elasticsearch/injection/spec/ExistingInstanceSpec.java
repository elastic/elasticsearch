/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.injection.spec;

public record ExistingInstanceSpec(Class<?> requestedType, Object instance) implements InjectionSpec {
    @Override
    public String toString() {
        // Don't call instance.toString; who knows what that will return
        return "ExistingInstanceSpec[" + "requestedType=" + requestedType + ']';
    }
}
