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

package org.elasticsearch.plugin.ingest;

import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.TemplateService;
import org.elasticsearch.ingest.processor.Processor;

/**
 * The ingest framework (pipeline, processor and processor factory) can't rely on ES specific code. However some
 * processors rely on reading files from the config directory. We can't add Environment as a constructor parameter,
 * so we need some code that provides the physical location of the configuration directory to the processor factories
 * that need this and this is what this processor factory provider does.
 */
@FunctionalInterface
interface ProcessorFactoryProvider {

    Processor.Factory get(Environment environment, TemplateService templateService);

}
