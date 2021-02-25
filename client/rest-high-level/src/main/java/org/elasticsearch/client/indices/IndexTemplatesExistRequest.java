/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import java.util.Arrays;
import java.util.List;

/**
 * A request to check for the existence of index templates
 */
public class IndexTemplatesExistRequest extends GetIndexTemplatesRequest {

    /**
     * Create a request to check for the existence of index templates. At least one template index name must be provided
     *
     * @param names the names of templates to check for the existence of
     */
    public IndexTemplatesExistRequest(String... names) {
        this(Arrays.asList(names));
    }

    /**
     * Create a request to check for the existence of index templates. At least one template index name must be provided
     *
     * @param names the names of templates to check for the existence of
     */
    public IndexTemplatesExistRequest(List<String> names) {
        super(names);
        if (names().isEmpty()) {
            throw new IllegalArgumentException("must provide at least one index template name");
        }
    }
}
