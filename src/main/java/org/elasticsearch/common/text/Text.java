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
package org.elasticsearch.common.text;

import org.elasticsearch.common.bytes.BytesReference;

import java.io.Serializable;

/**
 * Text represents a (usually) long text data. We use this abstraction instead of {@link String}
 * so we can represent it in a more optimized manner in memory as well as serializing it over the
 * network as well as converting it to json format.
 */
public interface Text extends Comparable<Text>, Serializable {

    /**
     * Are bytes available without the need to be converted into bytes when calling {@link #bytes()}.
     */
    boolean hasBytes();

    /**
     * The UTF8 bytes representing the the text, might be converted on the fly, see {@link #hasBytes()}
     */
    BytesReference bytes();

    /**
     * Is there a {@link String} representation of the text. If not, then it {@link #hasBytes()}.
     */
    boolean hasString();

    /**
     * Returns the string representation of the text, might be converted to a string on the fly.
     */
    String string();

    /**
     * Returns the string representation of the text, might be converted to a string on the fly.
     */
    @Override
    String toString();
}
