/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.aws.imds;

/**
 * Represents the IMDS protocol version simulated by the {@link Ec2ImdsHttpHandler}.
 */
public enum Ec2ImdsVersion {
    /**
     * Classic V1 behavior: plain {@code GET} requests, no tokens.
     */
    V1,

    /**
     * Newer V2 behavior: {@code GET} requests must include a {@code X-aws-ec2-metadata-token} header providing a token previously obtained
     * by calling {@code PUT /latest/api/token}.
     */
    V2
}
