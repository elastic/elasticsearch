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

package org.elasticsearch.search.aggregations.reducers.smooth.models;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.*;

public abstract class SmoothingModel {

    /**
     * Returns the next value in the series, according to the underlying smoothing model
     *
     * @param values    Collection of numerics to smooth, usually windowed
     * @param <T>       Type of numeric
     * @return          Returns a double, since most smoothing methods operate on floating points
     */
    public abstract <T extends Number> double next(Collection<T> values);

    /**
     * Write the model to the output stream
     *
     * @param out   Output stream
     * @throws IOException
     */
    public abstract void writeTo(StreamOutput out) throws IOException;
}




