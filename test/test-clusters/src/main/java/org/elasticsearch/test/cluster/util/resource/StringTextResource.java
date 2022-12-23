/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.util.resource;

class StringTextResource implements TextResource {
    private final String text;

    StringTextResource(String text) {
        this.text = text;
    }

    @Override
    public String getText() {
        return text;
    }
}
