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


package org.elasticsearch.ingest.processor;

import org.elasticsearch.ingest.Data;

import java.util.Map;

/**
 * A processor implementation may modify the data belonging to a document.
 * Whether changes are made and what exactly is modified is up to the implementation.
 */
public interface Processor {

    /**
     * Introspect and potentially modify the incoming data.
     */
    void execute(Data data);

    /**
     * A builder to construct a processor to be used in a pipeline.
     */
    interface Builder {

        /**
         * A general way to set processor related settings based on the config map.
         */
        void fromMap(Map<String, Object> config);

        /**
         * Builds the processor based on previous set settings.
         */
        Processor build();

        /**
         * A factory that creates a processor builder when processor instances for pipelines are being created.
         */
        interface Factory {

            /**
             * Creates the builder.
             */
            Builder create();

        }

    }

}
