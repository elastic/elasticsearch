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
package org.elasticsearch.common.io.stream;

import java.io.IOException;

/**
 * Implementers can be read from {@linkplain StreamInput} by calling their {@link #readFrom(StreamInput)} method.
 *
 * It is common for implementers of this interface to declare a <code>public static final</code> instance of themselves named PROTOTYPE so
 * users can call {@linkplain #readFrom(StreamInput)} on it. It is also fairly typical for readFrom to be implemented as a method that just
 * calls a constructor that takes {@linkplain StreamInput} as a parameter. This allows the fields in the implementer to be
 * <code>final</code>.
 */
public interface StreamableReader<T> {
    /**
     * Reads an object of this type from the provided {@linkplain StreamInput}. The receiving instance remains unchanged.
     */
    T readFrom(StreamInput in) throws IOException;
}
