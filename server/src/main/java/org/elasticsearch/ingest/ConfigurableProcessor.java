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

package org.elasticsearch.ingest;

import java.util.Map;

/**
 * Ingest processors may implement this interface to supply environment-specific metadata at pipeline creation time. This
 * metadata will be persisted in the pipeline's definition in cluster state and supplied to the processor's factory any
 * time the processor is created on ingest nodes in the cluster.
 */
public interface ConfigurableProcessor {

    /**
     * Processors must supply an identifier that uniquely identifies themselves based on their configuration.
     * @return Unique identifier
     */
    String getIdentifier();

    /**
     * @return Metadata to be persisted in pipeline definition for this processor.
     */
    Map<String, String> getMetadata();
}
