/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.common.Strings;

/**
 * A request to check for the existence of component templates
 */
public class ComponentTemplatesExistRequest extends GetComponentTemplatesRequest {

    /**
     * Create a request to check for the existence of component template. Name must be provided
     *
     * @param name the name of template to check for the existence of
     */
    public ComponentTemplatesExistRequest(String name) {
        super(name);
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("must provide component template name");
        }
    }
}
