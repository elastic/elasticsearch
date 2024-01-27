/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import java.util.function.Supplier;

/**
 * An internal plugin that will return a supplier of DocumentParsingObserver.
 */
public interface DocumentParsingObserverPlugin {

    /**
     * @return a supplier of DocumentParsingObserver to allow observing parsing events
     */
    Supplier<DocumentParsingObserver> getDocumentParsingObserverSupplier();
}
