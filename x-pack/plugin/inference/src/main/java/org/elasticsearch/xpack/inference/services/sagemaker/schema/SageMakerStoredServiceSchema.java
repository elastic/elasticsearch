/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

public interface SageMakerStoredServiceSchema extends ToXContentFragment, VersionedNamedWriteable {
    SageMakerStoredServiceSchema NO_OP = new SageMakerStoredServiceSchema() {
        private static final String NAME = "noop_sagemaker_service_schema";

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }
    };
}
