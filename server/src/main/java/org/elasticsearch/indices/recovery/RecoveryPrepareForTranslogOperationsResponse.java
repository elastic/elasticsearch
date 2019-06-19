/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Optional;

final class RecoveryPrepareForTranslogOperationsResponse extends TransportResponse {
    final Optional<Store.MetadataSnapshot> targetMetadata;

    RecoveryPrepareForTranslogOperationsResponse(Optional<Store.MetadataSnapshot> targetMetadata) {
        this.targetMetadata = targetMetadata;
    }

    RecoveryPrepareForTranslogOperationsResponse(final StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            targetMetadata = Optional.ofNullable(in.readOptionalWriteable(Store.MetadataSnapshot::new));
        } else {
            targetMetadata = Optional.empty();
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeOptionalWriteable(targetMetadata.orElse(null));
        }
    }
}
