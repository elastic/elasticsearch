/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin;

/**
 * A named plugin component. Components with a name can be registered and fetched under a name given in
 * <code>@NamedComponent</code>
 * @see NamedComponent
 */
public interface Nameable {

    /**
     * Returns a name from NamedComponent annotation.
     * @return a name used on NamedComponent annotation or null when a class implementing this interface is not annotated
     */
    default String name() {
        NamedComponent[] annotationsByType = this.getClass().getAnnotationsByType(NamedComponent.class);
        if (annotationsByType.length == 1) {
            return annotationsByType[0].value();
        }
        return null;
    }

}
