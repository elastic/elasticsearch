/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;

/**
 * Version that supports a {@link DataType}.
 */
interface CreatedVersion {
    boolean supports(StreamOutput out);

    CreatedVersion SUPPORTED_ON_ALL_NODES = new CreatedVersion() {
        @Override
        public boolean supports(StreamOutput out) {
            return true;
        }

        @Override
        public String toString() {
            return "SupportedOnAllVersions";
        }
    };

    static CreatedVersion supportedOn(TransportVersion createdVersion) {
        return new CreatedVersion() {
            @Override
            public boolean supports(StreamOutput out) {
                return out.getTransportVersion().supports(createdVersion);
            }

            @Override
            public String toString() {
                return "SupportedOn[" + createdVersion + "]";
            }
        };
    }
}
