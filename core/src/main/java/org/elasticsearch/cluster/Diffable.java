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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Cluster state part, changes in which can be serialized
 */
public interface Diffable<T> extends Writeable {

    /**
     * Returns serializable object representing differences between this and previousState
     */
    Diff<T> diff(T previousState);

    /**
     * Reads the {@link org.elasticsearch.cluster.Diff} from StreamInput
     */
    Diff<T> readDiffFrom(StreamInput in) throws IOException;

    /**
     * Reads an object of this type from the provided {@linkplain StreamInput}. The receiving instance remains unchanged.
     */
    T readFrom(StreamInput in) throws IOException;
}
