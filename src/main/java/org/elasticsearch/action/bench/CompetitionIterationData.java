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

package org.elasticsearch.action.bench;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 * Holds data points for a single benchmark iteration
 *
 * Note that the underlying data array may be actively populated, so we take care not
 * to compute statistics on uninitialized elements (which are indicated by a value of -1).
 * Initialized elements (those with values > -1) will not be changed once set.
 */
public class CompetitionIterationData implements Streamable {

    private long[] data;

    public CompetitionIterationData() { }

    public CompetitionIterationData(long[] data) {
        this.data = data;
    }

    public long[] data() {
        return data;
    }

    /**
     * The number of data values currently holding valid values
     *
     * @return      Number of data values currently holding valid values
     */
    public long length() {
        long length = 0;
        for (int i = 0; i < data.length; i++) {
            if (data[i] < 0) {  // Data values can be invalid when computing statistics on actively running benchmarks
                continue;
            }
            length++;
        }
        return length;
    }

    /**
     * The sum of all currently set values
     *
     * @return      The sum of all currently set values
     */
    public long sum() {
        long sum = 0;
        for (int i = 0; i < data.length; i++) {
            sum += Math.max(0, data[i]);  // Data values can be invalid when computing statistics on actively running benchmarks
        }
        return sum;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        data = in.readLongArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLongArray(data);
    }
}
