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

package org.elasticsearch.common.inject;

/**
 * This interface can be added to a Module to spawn sub modules. DO NOT USE.
 *
 * This is fundamentally broken.
 * <ul>
 * <li>If you have a plugin with multiple modules, return all the modules at once.</li>
 * <li>If you are trying to make the implementation of a module "pluggable", don't do it.
 * This is not extendable because custom implementations (using onModule) cannot be
 * registered before spawnModules() is called.</li>
 * </ul>
 */
public interface SpawnModules {

    Iterable<? extends Module> spawnModules();
}
