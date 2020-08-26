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

package org.elasticsearch.gradle;

import org.gradle.process.CommandLineArgumentProvider;

import java.util.Arrays;
import java.util.List;

/**
 * A {@link CommandLineArgumentProvider} implementation that simply returns the given list. This implementation does not track any
 * arguments as inputs, so this is useful for passing arguments that should not be used for the purposes of input snapshotting.
 */
public class SimpleCommandLineArgumentProvider implements CommandLineArgumentProvider {
    private final List<String> arguments;

    public SimpleCommandLineArgumentProvider(String... arguments) {
        this.arguments = Arrays.asList(arguments);
    }

    @Override
    public Iterable<String> asArguments() {
        return arguments;
    }
}
