/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;

public interface SupportedVersion {
    boolean supports(TransportVersion version);

    SupportedVersion SUPPORTED_ON_ALL_NODES = new SupportedVersion() {
        @Override
        public boolean supports(TransportVersion version) {
            return true;
        }

        @Override
        public String toString() {
            return "SupportedOnAllVersions";
        }
    };

    SupportedVersion UNDER_CONSTRUCTION = new SupportedVersion() {
        @Override
        public boolean supports(TransportVersion version) {
            return Build.current().isSnapshot();
        }

        @Override
        public String toString() {
            return "UnderConstruction";
        }
    };

    static SupportedVersion supportedOn(TransportVersion supportedVersion) {
        return new SupportedVersion() {
            @Override
            public boolean supports(TransportVersion version) {
                return Build.current().isSnapshot() || version.supports(supportedVersion);
            }

            @Override
            public String toString() {
                return "SupportedOn[" + supportedVersion + "]";
            }
        };
    }
}
