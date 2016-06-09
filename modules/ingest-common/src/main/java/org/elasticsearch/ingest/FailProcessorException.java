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

/**
 * Exception class thrown by {@link FailProcessor}.
 *
 * This exception is caught in the {@link org.elasticsearch.ingest.core.CompoundProcessor} and
 * then changes the state of {@link org.elasticsearch.ingest.core.IngestDocument}. This
 * exception should get serialized.
 */
public class FailProcessorException extends RuntimeException {

    public FailProcessorException(String message) {
        super(message);
    }
}

