/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cloud.jclouds;

import com.google.inject.Module;
import org.elasticsearch.cloud.jclouds.logging.JCloudsLoggingModule;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DynamicExecutors;
import org.jclouds.concurrent.config.ExecutorServiceModule;

import java.util.concurrent.Executors;

/**
 * @author kimchy (shay.banon)
 */
public class JCloudsUtils {

    public static Iterable<? extends Module> buildModules(Settings settings) {
        return ImmutableList.of(new JCloudsLoggingModule(settings),
                new ExecutorServiceModule(
                        Executors.newCachedThreadPool(DynamicExecutors.daemonThreadFactory(settings, "jclouds-user")),
                        Executors.newCachedThreadPool(DynamicExecutors.daemonThreadFactory(settings, "jclouds-io"))));
    }
}
