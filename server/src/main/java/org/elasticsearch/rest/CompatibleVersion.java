/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ParsedMediaType;

/**
 * An interface used to specify a function that returns a compatible API version.
 * This function abstracts how the version calculation is provided (for instance from plugin).
 */
@FunctionalInterface
public interface CompatibleVersion {
    Version get(@Nullable ParsedMediaType acceptHeader, @Nullable ParsedMediaType contentTypeHeader, boolean hasContent);

    CompatibleVersion CURRENT_VERSION = (acceptHeader, contentTypeHeader, hasContent) -> Version.CURRENT;
}
