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

import org.elasticsearch.common.Nullable;

import java.util.Map;

/**
 * A factory to create instances of either {@link ExecutableScript} or {@link SearchScript}. Note,
 * if this factory creates {@link SearchScript}, it must extend {@link AbstractSearchScript}.
 *
 * @see AbstractExecutableScript
 * @see AbstractSearchScript
 * @see AbstractLongSearchScript
 * @see AbstractDoubleSearchScript
 */
public interface NativeScriptFactory {

    /**
     * Creates a new instance of either a {@link ExecutableScript} or a {@link SearchScript}.
     *
     * @param params The parameters passed to the script. Can be <tt>null</tt>.
     */
    ExecutableScript newScript(@Nullable Map<String, Object> params);

    /**
     * Indicates if document scores may be needed by the produced scripts.
     *
     * @return {@code true} if scores are needed.
     */
    boolean needsScores();

    /**
     * Returns the name of the script factory
     */
    String getName();
}
