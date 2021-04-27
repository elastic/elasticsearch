/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;

public abstract class AbstractCharFilterFactory extends AbstractIndexComponent implements CharFilterFactory {

    private final String name;

    public AbstractCharFilterFactory(IndexSettings indexSettings, String name) {
        super(indexSettings);
        this.name = name;
    }

    @Override
    public String name() {
        return this.name;
    }
}

