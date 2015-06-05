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

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Represents difference between states of cluster state parts
 */
public interface Diff<T> {

    /**
     * Applies difference to the specified part and retunrs the resulted part
     */
    T apply(T part);

    /**
     * Writes the differences into the output stream
     * @param out
     * @throws IOException
     */
    void writeTo(StreamOutput out) throws IOException;
}
