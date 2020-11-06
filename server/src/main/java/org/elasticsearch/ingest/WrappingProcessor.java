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
 * A srapping processor is one that encapsulates an inner processor, or a processor that the wrapped processor enacts upon. All processors
 * that contain an "inner" processor should implement this interface, such that the actual processor can be obtained.
 */
public interface WrappingProcessor extends Processor {

    /**
     * Method for retrieving the inner processor from a wrapped processor.
     * @return the inner processor
     */
    Processor getInnerProcessor();
}
