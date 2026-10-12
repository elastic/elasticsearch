/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import org.elasticsearch.core.UpdateForV10;

/**
 * Called when resolving the S3 client region or endpoint uses a backwards-compatible guess that we intend to remove.
 */
@UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED) // in v10 we can fail on these branches with an IAE rather than emitting warnings
interface S3DeprecatedLeniencyHandler {
    /**
     * An endpoint was configured without a scheme prefix, which the SDK doesn't support any more so Elasticsearch had to guess.
     */
    void missingEndpointScheme(String configuredEndpoint, String endpointOverride);

    /**
     * An endpoint was configured which looks to be an AWS S3 regional endpoint, from which we have guessed the region to use when
     * signing requests.
     */
    void regionGuessedFromEndpoint(String configuredEndpoint, String guessedRegionId);

    /**
     * No region was specified, and we couldn't even guess one from the endpoint, so the only remaining option is to guess
     * {@code us-east-1}.
     */
    void regionGuessedAsUsEast1(String endpointDescription);

    /**
     * No region or endpoint was specified, or could be determined from the environment, so our only hope is to try cross-region access.
     */
    void regionFellBackToCrossRegionAccess(String endpointDescription);
}
