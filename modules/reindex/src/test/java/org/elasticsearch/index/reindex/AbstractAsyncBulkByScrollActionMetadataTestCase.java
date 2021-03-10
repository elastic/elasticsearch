/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

public abstract class AbstractAsyncBulkByScrollActionMetadataTestCase<
                Request extends AbstractBulkByScrollRequest<Request>,
                Response extends BulkByScrollResponse>
        extends AbstractAsyncBulkByScrollActionTestCase<Request, Response> {

    protected ScrollableHitSource.BasicHit doc() {
        return new ScrollableHitSource.BasicHit("index", "id", 0);
    }

    protected abstract AbstractAsyncBulkByScrollAction<Request, ?> action();
}
