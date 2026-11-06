/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import java.io.IOException;

/**
 * A run that is not what the engine wrote: a wrong magic or version, a chunk whose checksum does not match, a
 * truncated footer, an index entry out of range. It names the run, so the failure says which file to look at.
 */
public final class FramedRunCorruptionException extends IOException {

    public FramedRunCorruptionException(String run, String message) {
        super("spill run [" + run + "] is corrupt: " + message);
    }
}
