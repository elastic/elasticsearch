/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

/**
 * This bridge class exposes package-private components of Ingest in a way
 * that can be consumed by Logstash's Elastic Integration Filter without
 * expanding our externally-consumable API.
 *
 * @apiNote this is an Elastic-internal API bridge intended for exclusive use by
 *          Logstash and its Elastic Integration Filter.
 */
public class LogstashInternalBridge {

    private LogstashInternalBridge() {}

    /**
     * The document has been redirected to another target.
     * This implies that the default pipeline of the new target needs to be invoked.
     *
     * @return whether the document is redirected to another target
     */
    public static boolean isReroute(final IngestDocument ingestDocument) {
        return ingestDocument.isReroute();
    }

    /**
     * Set the reroute flag of the provided {@link IngestDocument} to {@code false}.
     */
    public static void resetReroute(final IngestDocument ingestDocument) {
        ingestDocument.resetReroute();
    }
}
