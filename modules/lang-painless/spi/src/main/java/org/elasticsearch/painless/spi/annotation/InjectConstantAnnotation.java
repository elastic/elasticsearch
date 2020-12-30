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

package org.elasticsearch.painless.spi.annotation;

import java.util.Collections;
import java.util.List;

/**
 * Inject compiler setting constants.
 * Format: {@code inject_constant["1=foo_compiler_setting", 2="bar_compiler_setting"]} injects "foo_compiler_setting and
 * "bar_compiler_setting" as the first two arguments (other than receiver reference for instance methods) to the annotated method.
 */
public class InjectConstantAnnotation {
    public static final String NAME = "inject_constant";
    public final List<String> injects;
    public InjectConstantAnnotation(List<String> injects) {
        this.injects = Collections.unmodifiableList(injects);
    }
}
