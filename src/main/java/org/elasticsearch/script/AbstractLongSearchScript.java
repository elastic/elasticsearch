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

package org.elasticsearch.script;

/**
 * A simpler base class instead of {@link AbstractSearchScript} for computations
 * that return a long number.
 */
public abstract class AbstractLongSearchScript extends AbstractSearchScript {

    @Override
    public Object run() {
        return runAsLong();
    }

    @Override
    public abstract long runAsLong();

    @Override
    public double runAsDouble() {
        return runAsLong();
    }

    @Override
    public float runAsFloat() {
        return runAsLong();
    }
}